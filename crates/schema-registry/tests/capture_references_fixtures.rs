//! Golden schema-references-lifecycle capture harness for Crabka Schema Registry.
//!
//! Boots a real `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0` container against an
//! in-process Crabka broker (same networking as `capture_admin_fixtures.rs`:
//! the broker binds `0.0.0.0:9092` and advertises `host.docker.internal:9092`,
//! while the host connects directly on `127.0.0.1:9092`), then drives the
//! schema-references lifecycle for all three formats (Avro, Protobuf, JSON):
//! for each format it registers a base schema, then a referrer that references
//! it, then captures the REST status + body that cp returns for the
//! referrer's id, the `references` array shape on `GET /schemas/ids/{id}`, the
//! `referencedby` shape, the delete-protection code when deleting a referenced
//! base, and the reference-not-found code for a dangling reference. Two
//! fixtures are produced:
//!
//!   * `tests/fixtures/references/rest.json`    — the REST status + parsed body
//!     that cp returns for every op in the references lifecycle (the oracle for
//!     ids / codes / `referencedby` shape of the references slice).
//!   * `tests/fixtures/references/records.json` — the exact `(offset, key,
//!     value)` bytes cp wrote to the `_schemas` topic over the lifecycle,
//!     dumped by fetching partition 0 directly from the host broker (the goal
//!     is the SCHEMA values carrying a non-empty `references` array).
//!
//! ```text
//! cargo test -p crabka-schema-registry --test capture_references_fixtures -- --ignored --nocapture
//! ```
//!
//! Re-running this test regenerates both fixture files verbatim.

#![allow(clippy::pedantic)]

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

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

fn references_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("references")
}

fn write_references_fixture(name: &str, body: &str) {
    let dir = references_fixtures_dir();
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

// ── references lifecycle driver ─────────────────────────────────────────────────

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

/// Extract the integer schema id from a recorded REST result's body, panicking
/// with the op label + body if it is absent (e.g. cp returned an error). Used
/// to thread a registered referrer's id into the subsequent `GET
/// /schemas/ids/{id}` lookup.
fn id_from(result: &serde_json::Value) -> i64 {
    result["body"]["id"].as_i64().unwrap_or_else(|| {
        panic!(
            "expected integer `id` in body of op {:?}, got {}",
            result["op"], result["body"]
        )
    })
}

/// Drive the references lifecycle for one format and append every op's REST
/// result to `results`.
///
/// `prefix` namespaces the subjects (e.g. `av_`). `base_subject` /
/// `referrer_subject` are the two subjects; `base_body` registers the base
/// schema and `referrer_body` registers the referrer (whose `references` array
/// points at `base_subject` version 1). `missing_body` registers a schema with
/// a dangling reference to a non-existent subject (the reference-not-found
/// path). The sequence per format is:
///
///   1. register base               → its id
///   2. register referrer (w/ refs) → its id (proves refs identity)
///   3. GET /schemas/ids/{referrer} → the `references` array shape
///   4. GET .../{base}/v1/referencedby → expected `[referrer_id]`
///   5. DELETE referenced base v1    → the delete-protection code (~42206)
///   6. register dangling-ref schema → the reference-not-found code
#[allow(clippy::too_many_arguments)]
async fn run_format_lifecycle(
    http: &reqwest::Client,
    base: &str,
    results: &mut Vec<serde_json::Value>,
    fmt: &str,
    base_subject: &str,
    referrer_subject: &str,
    bad_subject: &str,
    base_body: serde_json::Value,
    referrer_body: serde_json::Value,
    missing_body: serde_json::Value,
) {
    // 1. Register the base schema (its first version is v1).
    results.push(
        drive(
            http,
            base,
            &format!("{fmt}_register_base"),
            "POST",
            &format!("/subjects/{base_subject}/versions"),
            Some(base_body),
        )
        .await,
    );

    // 2. Register the referrer that references the base at version 1.
    results.push(
        drive(
            http,
            base,
            &format!("{fmt}_register_referrer"),
            "POST",
            &format!("/subjects/{referrer_subject}/versions"),
            Some(referrer_body),
        )
        .await,
    );
    // Thread the referrer's id into the by-id lookup below.
    let referrer_id = id_from(results.last().unwrap());

    // 3. GET the referrer by id — captures the `references` array shape.
    results.push(
        drive(
            http,
            base,
            &format!("{fmt}_get_referrer_by_id"),
            "GET",
            &format!("/schemas/ids/{referrer_id}"),
            None,
        )
        .await,
    );

    // 4. referencedby for the base v1 — expected `[referrer_id]`.
    results.push(
        drive(
            http,
            base,
            &format!("{fmt}_base_v1_referencedby"),
            "GET",
            &format!("/subjects/{base_subject}/versions/1/referencedby"),
            None,
        )
        .await,
    );

    // 5. DELETE the referenced base v1 — captures the delete-protection code.
    results.push(
        drive(
            http,
            base,
            &format!("{fmt}_delete_referenced_base"),
            "DELETE",
            &format!("/subjects/{base_subject}/versions/1"),
            None,
        )
        .await,
    );

    // 6. Register a schema with a dangling reference — reference-not-found code.
    results.push(
        drive(
            http,
            base,
            &format!("{fmt}_register_missing_ref"),
            "POST",
            &format!("/subjects/{bad_subject}/versions"),
            Some(missing_body),
        )
        .await,
    );
}

/// Drive the full references lifecycle for all three formats, recording every
/// op's REST result into the returned vector (→ `rest.json`).
async fn run_references_lifecycle(http: &reqwest::Client, base: &str) -> Vec<serde_json::Value> {
    let mut results: Vec<serde_json::Value> = Vec::new();

    // ── Avro ────────────────────────────────────────────────────────────────
    run_format_lifecycle(
        http,
        base,
        &mut results,
        "avro",
        "av_money",
        "av_order",
        "av_bad",
        serde_json::json!({
            "schema": "{\"type\":\"record\",\"name\":\"Money\",\"fields\":[{\"name\":\"cents\",\"type\":\"long\"}]}"
        }),
        serde_json::json!({
            "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"price\",\"type\":\"Money\"}]}",
            "references": [{ "name": "Money", "subject": "av_money", "version": 1 }]
        }),
        serde_json::json!({
            "schema": "{\"type\":\"record\",\"name\":\"Bad\",\"fields\":[{\"name\":\"x\",\"type\":\"Missing\"}]}",
            "references": [{ "name": "Missing", "subject": "nope", "version": 1 }]
        }),
    )
    .await;

    // ── Protobuf ──────────────────────────────────────────────────────────────
    run_format_lifecycle(
        http,
        base,
        &mut results,
        "protobuf",
        "pb_money",
        "pb_order",
        "pb_bad",
        serde_json::json!({
            "schemaType": "PROTOBUF",
            "schema": "syntax=\"proto3\"; package m; message Money{int64 cents=1;}"
        }),
        serde_json::json!({
            "schemaType": "PROTOBUF",
            "schema": "syntax=\"proto3\"; import \"money.proto\"; message Order{m.Money price=1;}",
            "references": [{ "name": "money.proto", "subject": "pb_money", "version": 1 }]
        }),
        serde_json::json!({
            "schemaType": "PROTOBUF",
            "schema": "syntax=\"proto3\"; import \"missing.proto\"; message Bad{m.Missing x=1;}",
            "references": [{ "name": "missing.proto", "subject": "nope", "version": 1 }]
        }),
    )
    .await;

    // ── JSON ──────────────────────────────────────────────────────────────────
    run_format_lifecycle(
        http,
        base,
        &mut results,
        "json",
        "js_amount",
        "js_order",
        "js_bad",
        serde_json::json!({
            "schemaType": "JSON",
            "schema": "{\"type\":\"integer\",\"maximum\":10}"
        }),
        serde_json::json!({
            "schemaType": "JSON",
            "schema": "{\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"Amount\"}}}",
            "references": [{ "name": "Amount", "subject": "js_amount", "version": 1 }]
        }),
        serde_json::json!({
            "schemaType": "JSON",
            "schema": "{\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"Missing\"}}}",
            "references": [{ "name": "Missing", "subject": "nope", "version": 1 }]
        }),
    )
    .await;

    results
}

// ── `_schemas` byte dump ────────────────────────────────────────────────────────

/// Connect host-side directly to `127.0.0.1:9092` and dump every `_schemas`
/// partition-0 record's `(offset, key, value)` bytes (UTF-8 lossy) to
/// `records.json`. `fetch_partition` fetches over the given connection with no
/// leader re-routing, so the direct (non-advertised) connection works. The
/// goal is to capture the SCHEMA values carrying a non-empty `references` array.
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
            client_id: "references-capture".into(),
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

    write_references_fixture("records.json", &serde_json::to_string_pretty(&out).unwrap());
    eprintln!("CAPTURE _schemas dump: {} records", out.len());
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures cp-schema-registry references fixtures"]
async fn capture_references_lifecycle() {
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

    // Drive the references lifecycle across all three formats and persist the
    // REST verdicts (ids / codes / `referencedby` shape).
    let results = run_references_lifecycle(&http, &base).await;
    write_references_fixture(
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
