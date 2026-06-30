//! Golden admin-lifecycle capture harness for Crabka Schema Registry slice 3.
//!
//! Boots a real `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0` container against an
//! in-process Crabka broker (same networking as `capture_compat_fixtures.rs`:
//! the broker binds `0.0.0.0:9092` and advertises `host.docker.internal:9092`,
//! while the host connects directly on `127.0.0.1:9092`), then drives the
//! delete / mode / lookup admin lifecycle against cp's REST API. Two fixtures
//! are produced:
//!
//!   * `tests/fixtures/admin/rest.json`    — the REST status + parsed body that
//!     cp returns for every op in the lifecycle (the oracle for the error-code
//!     and status calibration of slice 3).
//!   * `tests/fixtures/admin/records.json` — the exact `(offset, key, value)`
//!     bytes cp wrote to the `_schemas` topic over the lifecycle, dumped by
//!     fetching partition 0 directly from the host broker.
//!
//! ```text
//! cargo test -p crabka-schema-registry --test capture_admin_fixtures -- --ignored --nocapture
//! ```
//!
//! Re-running this test regenerates both fixture files verbatim.

#![allow(clippy::pedantic)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crabka_broker::{Broker, BrokerConfig};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

/// The broker binds host port 9092 and cp-schema-registry reaches it via
/// `host.docker.internal:9092` (container network) while the host connects
/// directly on `127.0.0.1:9092`.
const LISTEN: &str = "0.0.0.0:9092";
const CONTROLLER_LISTEN: &str = "0.0.0.0:9093";
const ADVERTISED: &str = "host.docker.internal:9092";

const SR_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0";
const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

// ── fixture paths ─────────────────────────────────────────────────────────────

fn admin_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("admin")
}

fn write_admin_fixture(name: &str, body: &str) {
    let dir = admin_fixtures_dir();
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
        node_id: 1,
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(1, controller_addr.to_string())],
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
    assert!(
        out.status.success(),
        "docker pull {image} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

fn docker_run_schema_registry() -> String {
    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            "0:8081",
            "-e",
            "SCHEMA_REGISTRY_HOST_NAME=localhost",
            "-e",
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:9092",
            "-e",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
            SR_IMAGE,
        ])
        .output()
        .expect("spawn docker run schema-registry");
    assert!(
        out.status.success(),
        "docker run schema-registry failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(!id.is_empty(), "empty container id from docker run");
    eprintln!("CAPTURE schema-registry container id={id}");
    id
}

fn docker_mapped_port(id: &str) -> u16 {
    let out = Command::new("docker")
        .args(["port", id, "8081"])
        .output()
        .expect("spawn docker port");
    assert!(
        out.status.success(),
        "docker port {id} 8081 failed: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
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

// ── REST helpers ──────────────────────────────────────────────────────────────

async fn wait_for_registry(http: &reqwest::Client, base: &str, container_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    let url = format!("{base}/subjects");
    let mut last: Option<String> = None;
    while Instant::now() < deadline {
        match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                eprintln!("CAPTURE schema-registry READY ({})", resp.status());
                return;
            }
            Ok(resp) => last = Some(format!("status {}", resp.status())),
            Err(e) => last = Some(format!("err {e}")),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let logs = docker_logs(container_id);
    panic!(
        "schema-registry never became ready within 120s (last: {last:?}).\ncontainer logs:\n{logs}"
    );
}

// ── admin lifecycle driver ──────────────────────────────────────────────────────

/// Perform one REST request and return a JSON record of it (op label, method,
/// path, HTTP status, and parsed-or-raw body) for `rest.json`.
async fn drive(
    http: &reqwest::Client,
    base: &str,
    op: &str,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
) -> serde_json::Value {
    let url = format!("{base}{path}");
    let rb = match method {
        "GET" => http.get(&url),
        "POST" => http.post(&url),
        "PUT" => http.put(&url),
        "DELETE" => http.delete(&url),
        m => panic!("unsupported method {m}"),
    };
    let rb = rb.header("Content-Type", SR_CONTENT_TYPE);
    let rb = if let Some(b) = &body {
        rb.body(serde_json::to_string(b).unwrap())
    } else {
        rb
    };
    let resp = rb
        .send()
        .await
        .unwrap_or_else(|e| panic!("{method} {url}: {e}"));
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    let parsed =
        serde_json::from_str::<serde_json::Value>(&text).unwrap_or(serde_json::Value::String(text));
    eprintln!("CAPTURE {op}: {method} {path} -> {status} {parsed}");
    serde_json::json!({
        "op": op,
        "method": method,
        "path": path,
        "status": status,
        "body": parsed,
    })
}

/// Minimal Avro record schema with the given name and no fields.
fn av(n: &str) -> String {
    format!("{{\"type\":\"record\",\"name\":\"{n}\",\"fields\":[]}}")
}

/// Drive the full delete / mode / lookup lifecycle, recording every op's REST
/// result into the returned vector (→ `rest.json`).
async fn run_admin_lifecycle(http: &reqwest::Client, base: &str) -> Vec<serde_json::Value> {
    let mut results: Vec<serde_json::Value> = Vec::new();

    macro_rules! step {
        ($op:expr, $method:expr, $path:expr) => {
            results.push(drive(http, base, $op, $method, $path, None).await)
        };
        ($op:expr, $method:expr, $path:expr, $body:expr) => {
            results.push(drive(http, base, $op, $method, $path, Some($body)).await)
        };
    }

    // 1. Force compat=NONE on subject `t` so two distinct schemas register as v1,v2.
    step!(
        "config_t_none",
        "PUT",
        "/config/t",
        serde_json::json!({ "compatibility": "NONE" })
    );
    // 2. Register schema A as v1.
    step!(
        "register_t_v1",
        "POST",
        "/subjects/t/versions",
        serde_json::json!({ "schema": av("A") })
    );
    // 3. Register schema B as v2.
    step!(
        "register_t_v2",
        "POST",
        "/subjects/t/versions",
        serde_json::json!({ "schema": av("B") })
    );
    // 4. Soft-delete version 1.
    step!("soft_delete_t_v1", "DELETE", "/subjects/t/versions/1");
    // 5. List versions (soft-deleted excluded).
    step!("list_t_versions", "GET", "/subjects/t/versions");
    // 6. List versions including soft-deleted.
    step!(
        "list_t_versions_deleted",
        "GET",
        "/subjects/t/versions?deleted=true"
    );
    // 7. Get soft-deleted v1 without ?deleted (expect 404 — VERSION-soft-deleted code).
    step!("get_t_v1_after_soft", "GET", "/subjects/t/versions/1");
    // 8. Get soft-deleted v1 WITH ?deleted (expect 200).
    step!(
        "get_t_v1_after_soft_deleted",
        "GET",
        "/subjects/t/versions/1?deleted=true"
    );
    // 9. Permanent-delete v2 with NO prior soft (expect error — VERSION-not-soft-deleted code).
    step!(
        "perm_delete_t_v2_no_soft",
        "DELETE",
        "/subjects/t/versions/2?permanent=true"
    );
    // 10. Permanent-delete v1 after soft.
    step!(
        "perm_delete_t_v1_after_soft",
        "DELETE",
        "/subjects/t/versions/1?permanent=true"
    );
    // 11. Register schema A under subject `d`.
    step!(
        "register_d_v1",
        "POST",
        "/subjects/d/versions",
        serde_json::json!({ "schema": av("A") })
    );
    // 12. Permanent-delete subject `d` with NO prior soft (expect error — SUBJECT-not-soft-deleted code).
    step!(
        "perm_delete_d_no_soft",
        "DELETE",
        "/subjects/d?permanent=true"
    );
    // 13. Soft-delete subject `d`.
    step!("soft_delete_d", "DELETE", "/subjects/d");
    // 14. Soft-delete subject `d` AGAIN (expect error — already-SUBJECT-soft-deleted code).
    step!("soft_delete_d_again", "DELETE", "/subjects/d");
    // 15. Permanent-delete subject `d` after soft.
    step!(
        "perm_delete_d_after_soft",
        "DELETE",
        "/subjects/d?permanent=true"
    );
    // 16. Set subject `r` mode READONLY.
    step!(
        "mode_r_readonly",
        "PUT",
        "/mode/r",
        serde_json::json!({ "mode": "READONLY" })
    );
    // 17. Register under READONLY subject `r` (expect rejected — operation-not-permitted code).
    step!(
        "register_r_readonly",
        "POST",
        "/subjects/r/versions",
        serde_json::json!({ "schema": av("A") })
    );
    // 18. Get the global mode.
    step!("get_mode_global", "GET", "/mode");
    // 19. Get subject `r` mode override.
    step!("get_mode_r", "GET", "/mode/r");
    // 20. Get mode of a subject with no override (captures cp's 404-vs-effective behavior).
    step!("get_mode_nope", "GET", "/mode/nope");
    // 21. Delete subject `r` mode override.
    step!("delete_mode_r", "DELETE", "/mode/r");
    // 22. Set subject `i` mode IMPORT.
    step!(
        "mode_i_import",
        "PUT",
        "/mode/i",
        serde_json::json!({ "mode": "IMPORT" })
    );
    // 23. Register under IMPORT subject `i` with explicit id + version.
    step!(
        "register_i_import",
        "POST",
        "/subjects/i/versions",
        serde_json::json!({ "schema": av("C"), "id": 42, "version": 5 })
    );
    // 24. List all schemas.
    step!("get_schemas", "GET", "/schemas");
    // 25. Reverse-lookup subjects/versions for id 1.
    step!("schemas_id1_versions", "GET", "/schemas/ids/1/versions");
    // 26. Referenced-by for subject `i` version 5.
    step!(
        "i_v5_referencedby",
        "GET",
        "/subjects/i/versions/5/referencedby"
    );

    results
}

// ── `_schemas` byte dump ────────────────────────────────────────────────────────

/// Connect host-side directly to `127.0.0.1:9092` and dump every `_schemas`
/// partition-0 record's `(offset, key, value)` bytes (UTF-8 lossy) to
/// `records.json`. `fetch_partition` fetches over the given connection with no
/// leader re-routing, so the direct (non-advertised) connection works.
async fn dump_schemas_records() {
    // Resolve the `_schemas` topic_id.
    let mut admin = crabka_client_admin::AdminClient::connect(&["127.0.0.1:9092".to_string()])
        .await
        .expect("admin connect");
    let md = admin.metadata(&["_schemas"]).await.expect("metadata");
    let topic_id = md
        .topics
        .into_iter()
        .find(|t| t.name == "_schemas")
        .and_then(|t| t.topic_id)
        .map(|id| WireUuid(*id.as_bytes()))
        .expect("_schemas topic_id");

    // Fetch all records from offset 0 over a direct connection.
    let addr: SocketAddr = "127.0.0.1:9092".parse().expect("static addr");
    let conn = crabka_client_core::Connection::connect_with_options(
        addr,
        crabka_client_core::ConnectionOptions {
            client_id: "admin-capture".into(),
            ..Default::default()
        },
    )
    .await
    .expect("connect");

    let mut out: Vec<serde_json::Value> = Vec::new();
    let mut next = 0_i64;
    loop {
        let recs =
            crabka_client_core::fetch_partition(&conn, "_schemas", topic_id, 0, next, 500, 1 << 20)
                .await
                .expect("fetch");
        if recs.is_empty() {
            break;
        }
        for r in &recs {
            out.push(serde_json::json!({
                "offset": r.offset,
                "key": r.key.as_deref().map(|k| String::from_utf8_lossy(k).to_string()),
                "value": r.value.as_deref().map(|v| String::from_utf8_lossy(v).to_string()),
            }));
            next = r.offset + 1;
        }
    }
    conn.close();

    write_admin_fixture("records.json", &serde_json::to_string_pretty(&out).unwrap());
    eprintln!("CAPTURE _schemas dump: {} records", out.len());
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures cp-schema-registry admin record bytes + REST codes"]
async fn capture_admin_lifecycle() {
    docker_pull(SR_IMAGE);

    let (broker, _dir) = start_host_broker().await;

    let container_id = docker_run_schema_registry();
    let _guard = ContainerGuard {
        id: container_id.clone(),
    };

    let port = docker_mapped_port(&container_id);
    let base = format!("http://127.0.0.1:{port}");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    wait_for_registry(&http, &base, &container_id).await;

    // Drive the 26-op admin lifecycle and persist the REST verdicts.
    let results = run_admin_lifecycle(&http, &base).await;
    write_admin_fixture(
        "rest.json",
        &serde_json::to_string_pretty(&results).unwrap(),
    );

    // Dump the exact `_schemas` record bytes cp emitted (broker still up).
    dump_schemas_records().await;

    broker.shutdown().await;
    eprintln!(
        "CAPTURE done — rest.json has {} ops; records.json written",
        results.len()
    );
}
