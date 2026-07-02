//! Golden compatibility-verdict capture harness for Crabka Schema Registry slice 2.
//!
//! Boots a real `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0` container against an
//! in-process Crabka broker (same networking as `capture_fixtures.rs`), then
//! drives the compatibility check API for 7 Avro cases × 3 compatibility
//! levels = 21 entries. The verdicts are written to:
//!
//!   `tests/fixtures/compat/avro_matrix.json`
//!
//! That file is the oracle for Task 8's conformance tests.
//!
//! ```text
//! cargo test -p crabka-schema-registry --test capture_compat_fixtures -- --ignored --nocapture
//! ```
//!
//! Re-running this test regenerates the fixture file verbatim.

#![allow(clippy::pedantic)]

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use crabka_broker::{Broker, BrokerConfig};

/// The broker binds host port 9092 and cp-schema-registry reaches it via
/// `host.docker.internal:9092` (container network) while the host connects
/// directly on `127.0.0.1:9092`.
const LISTEN: &str = "0.0.0.0:9092";
const CONTROLLER_LISTEN: &str = "0.0.0.0:9093";
const ADVERTISED: &str = "host.docker.internal:9092";

const SR_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0";
const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

// ── fixture paths ─────────────────────────────────────────────────────────────

fn compat_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("compat")
}

fn write_compat_fixture(name: &str, body: &str) {
    let dir = compat_fixtures_dir();
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

/// PUT /config/{subject} to set the compatibility level.
async fn set_subject_compat(http: &reqwest::Client, base: &str, subject: &str, level: &str) {
    let url = format!("{base}/config/{subject}");
    let body = serde_json::json!({ "compatibility": level });
    let resp = http
        .put(&url)
        .header("Content-Type", SR_CONTENT_TYPE)
        .body(serde_json::to_string(&body).expect("serialize compat body"))
        .send()
        .await
        .unwrap_or_else(|e| panic!("PUT {url} failed: {e}"));
    let status = resp.status();
    let text = resp
        .text()
        .await
        .unwrap_or_else(|e| panic!("read body of PUT {url}: {e}"));
    assert!(
        status.is_success(),
        "PUT /config/{subject} returned {status}: {text}"
    );
    eprintln!("CAPTURE set {subject} compat={level}");
}

/// POST /subjects/{subject}/versions to register writer as v1.
async fn register_writer(http: &reqwest::Client, base: &str, subject: &str, schema: &str) {
    let url = format!("{base}/subjects/{subject}/versions");
    let body = serde_json::json!({ "schema": schema });
    let resp = http
        .post(&url)
        .header("Content-Type", SR_CONTENT_TYPE)
        .body(serde_json::to_string(&body).expect("serialize register body"))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {url} failed: {e}"));
    let status = resp.status();
    let text = resp
        .text()
        .await
        .unwrap_or_else(|e| panic!("read body of POST {url}: {e}"));
    assert!(
        status.is_success(),
        "register writer for {subject} returned {status}: {text}"
    );
    eprintln!("CAPTURE registered writer for {subject}: {text}");
}

/// POST /compatibility/subjects/{subject}/versions/latest — returns is_compatible.
async fn check_compat(http: &reqwest::Client, base: &str, subject: &str, reader: &str) -> bool {
    let url = format!("{base}/compatibility/subjects/{subject}/versions/latest");
    let body = serde_json::json!({ "schema": reader });
    let resp = http
        .post(&url)
        .header("Content-Type", SR_CONTENT_TYPE)
        .body(serde_json::to_string(&body).expect("serialize compat check body"))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {url} failed: {e}"));
    let status = resp.status();
    let text = resp
        .text()
        .await
        .unwrap_or_else(|e| panic!("read body of POST {url}: {e}"));
    assert!(
        status.is_success(),
        "compat check for {subject} returned {status}: {text}"
    );
    let v: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse compat response {text}: {e}"));
    let result = v
        .get("is_compatible")
        .and_then(|x| x.as_bool())
        .unwrap_or_else(|| panic!("no bool `is_compatible` in {text}"));
    eprintln!("CAPTURE {subject} is_compatible={result}");
    result
}

// ── Avro case definitions ─────────────────────────────────────────────────────

struct CompatCase {
    name: &'static str,
    writer: &'static str,
    reader: &'static str,
}

fn avro_cases() -> Vec<CompatCase> {
    // Base record: R = {type:record, name:U, fields:[{name:id, type:int}]}
    const R: &str = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#;

    vec![
        // 1. add field with default  (reader adds x:int default 0)
        CompatCase {
            name: "add_default",
            writer: R,
            reader: r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int","default":0}]}"#,
        },
        // 2. add field without default
        CompatCase {
            name: "add_nodef",
            writer: R,
            reader: r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int"}]}"#,
        },
        // 3. remove field (reader drops id entirely)
        CompatCase {
            name: "remove_field",
            writer: R,
            reader: r#"{"type":"record","name":"U","fields":[]}"#,
        },
        // 4. promote int -> long (writer:int, reader:long)
        CompatCase {
            name: "promote_int_long",
            writer: R,
            reader: r#"{"type":"record","name":"U","fields":[{"name":"id","type":"long"}]}"#,
        },
        // 5. narrow long -> int (writer:long, reader:int)
        CompatCase {
            name: "narrow_long_int",
            writer: r#"{"type":"record","name":"U","fields":[{"name":"id","type":"long"}]}"#,
            reader: R,
        },
        // 6. enum add symbol (writer: E[A], reader: E[A,B])
        CompatCase {
            name: "enum_add",
            writer: r#"{"type":"record","name":"U","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}}]}"#,
            reader: r#"{"type":"record","name":"U","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}"#,
        },
        // 7. enum remove symbol (writer: E[A,B], reader: E[A])
        CompatCase {
            name: "enum_remove",
            writer: r#"{"type":"record","name":"U","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}"#,
            reader: r#"{"type":"record","name":"U","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}}]}"#,
        },
    ]
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures golden Avro compatibility fixtures"]
async fn capture_avro_compat_matrix() {
    docker_pull(SR_IMAGE);

    let (broker, _dir) = start_host_broker().await;

    let container_id = docker_run_schema_registry();
    let _guard = ContainerGuard {
        id: container_id.clone(),
    };

    let matrix = run_compat_capture(&container_id).await;

    // Sanity: exactly 21 entries (7 cases × 3 levels).
    assert_eq!(
        matrix.len(),
        21,
        "expected 21 matrix entries, got {}",
        matrix.len()
    );

    let json = serde_json::to_string_pretty(&matrix).expect("serialize matrix");
    write_compat_fixture("avro_matrix.json", &json);

    broker.shutdown().await;
    eprintln!(
        "CAPTURE done — avro_matrix.json has {} entries",
        matrix.len()
    );
}

/// Drive the compatibility API and return the result vector.
async fn run_compat_capture(container_id: &str) -> Vec<serde_json::Value> {
    let port = docker_mapped_port(container_id);
    let base = format!("http://127.0.0.1:{port}");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    wait_for_registry(&http, &base, container_id).await;

    let cases = avro_cases();
    let levels = ["BACKWARD", "FORWARD", "FULL"];
    let mut results: Vec<serde_json::Value> = Vec::new();

    for case in &cases {
        for level in levels {
            let subject = format!("{}-{}", case.name, level.to_lowercase());

            // 1. Set compatibility level for this subject.
            set_subject_compat(&http, &base, &subject, level).await;

            // 2. Register writer schema as v1.
            register_writer(&http, &base, &subject, case.writer).await;

            // 3. Check if reader is compatible with the registered writer.
            let is_compatible = check_compat(&http, &base, &subject, case.reader).await;

            eprintln!(
                "CAPTURE case={} level={} is_compatible={}",
                case.name, level, is_compatible
            );

            results.push(serde_json::json!({
                "case": case.name,
                "level": level,
                "writer": case.writer,
                "reader": case.reader,
                "is_compatible": is_compatible,
            }));
        }
    }

    results
}
