//! Golden compatibility-verdict capture harness for Crabka Schema Registry slice 2b.
//!
//! Boots a real `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0` container against an
//! in-process Crabka broker, then drives the compatibility check API for ~30
//! Protobuf cases × 3 compatibility levels ≈ 90 entries. Verdicts are written to:
//!
//!   `tests/fixtures/compat/protobuf_matrix.json`
//!
//! That file is the oracle for Task 6's calibration.
//!
//! ```text
//! cargo test -p crabka-schema-registry --test capture_protobuf_fixtures -- --ignored --nocapture
//! ```
//!
//! Re-running this test regenerates the fixture file verbatim.

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
    assert2::assert!(out.status.success());
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert2::assert!(!id.is_empty());
    eprintln!("CAPTURE schema-registry container id={id}");
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

// ── REST helpers ──────────────────────────────────────────────────────────────

async fn wait_for_registry(http: &reqwest::Client, base: &str, container_id: &str) {
    let deadline = Instant::now() + Duration::from_mins(2);
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
    let _text = resp
        .text()
        .await
        .unwrap_or_else(|e| panic!("read body of PUT {url}: {e}"));
    assert2::assert!(status.is_success());
    eprintln!("CAPTURE set {subject} compat={level}");
}

/// POST /subjects/{subject}/versions to register writer as v1.
/// Returns `Ok(())` on success, `Err(error_text)` if cp rejects it (caller logs + skips).
async fn try_register_writer(
    http: &reqwest::Client,
    base: &str,
    subject: &str,
    schema: &str,
) -> Result<(), String> {
    let url = format!("{base}/subjects/{subject}/versions");
    let body = serde_json::json!({ "schema": schema, "schemaType": "PROTOBUF" });
    let resp = http
        .post(&url)
        .header("Content-Type", SR_CONTENT_TYPE)
        .body(serde_json::to_string(&body).expect("serialize register body"))
        .send()
        .await
        .map_err(|e| format!("POST {url} network error: {e}"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("read body of POST {url}: {e}"))?;
    if status.is_success() {
        eprintln!("CAPTURE registered writer for {subject}: {text}");
        Ok(())
    } else {
        Err(format!(
            "register writer for {subject} returned {status}: {text}"
        ))
    }
}

/// POST /compatibility/subjects/{subject}/versions/latest — returns `Ok(is_compatible)` or
/// `Err(error_text)` if cp returns a non-success status (e.g. 500 Internal Server Error on
/// certain oneof transitions — a known cp bug; caller logs + skips).
async fn try_check_compat(
    http: &reqwest::Client,
    base: &str,
    subject: &str,
    reader: &str,
) -> Result<bool, String> {
    let url = format!("{base}/compatibility/subjects/{subject}/versions/latest");
    let body = serde_json::json!({ "schema": reader, "schemaType": "PROTOBUF" });
    let resp = http
        .post(&url)
        .header("Content-Type", SR_CONTENT_TYPE)
        .body(serde_json::to_string(&body).expect("serialize compat check body"))
        .send()
        .await
        .map_err(|e| format!("POST {url} network error: {e}"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("read body of POST {url}: {e}"))?;
    if !status.is_success() {
        return Err(format!(
            "compat check for {subject} returned {status}: {text}"
        ));
    }
    let v: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse compat response {text}: {e}"));
    let result = v
        .get("is_compatible")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or_else(|| panic!("no bool `is_compatible` in {text}"));
    eprintln!("CAPTURE {subject} is_compatible={result}");
    Ok(result)
}

// ── Protobuf case definitions ─────────────────────────────────────────────────

struct CompatCase {
    name: &'static str,
    writer: &'static str,
    reader: &'static str,
}

fn protobuf_cases() -> Vec<CompatCase> {
    let mut cases = protobuf_field_cases();
    cases.extend(protobuf_advanced_cases());
    cases
}

fn protobuf_field_cases() -> Vec<CompatCase> {
    vec![
        // 1. field_added: reader adds a new optional field
        CompatCase {
            name: "field_added",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { int32 id = 1; int32 x = 2; }",
        },
        // 2. field_removed: reader drops field x
        CompatCase {
            name: "field_removed",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; int32 x = 2; }",
            reader: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
        },
        // 3. scalar_int_widen: int32 -> int64 (compatible wire types, varint group)
        CompatCase {
            name: "scalar_int_widen",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { int64 id = 1; }",
        },
        // 4. scalar_int_to_string: int32 -> string (incompatible wire types)
        CompatCase {
            name: "scalar_int_to_string",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { string id = 1; }",
        },
        // 5. scalar_sint_group: sint32 -> sint64 (both sint, varint group)
        CompatCase {
            name: "scalar_sint_group",
            writer: "syntax = \"proto3\";\nmessage U { sint32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { sint64 id = 1; }",
        },
        // 6. scalar_string_bytes: string -> bytes (same wire type 2, length-delimited)
        CompatCase {
            name: "scalar_string_bytes",
            writer: "syntax = \"proto3\";\nmessage U { string id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { bytes id = 1; }",
        },
        // 7. scalar_fixed32_group: fixed32 -> sfixed32 (both 32-bit fixed)
        CompatCase {
            name: "scalar_fixed32_group",
            writer: "syntax = \"proto3\";\nmessage U { fixed32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { sfixed32 id = 1; }",
        },
        // 8. scalar_int_to_sint: int32 -> sint32 (incompatible varint encoding)
        CompatCase {
            name: "scalar_int_to_sint",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { sint32 id = 1; }",
        },
        // 9. kind_scalar_to_msg: field type changes from scalar to message
        CompatCase {
            name: "kind_scalar_to_msg",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage M {}\nmessage U { M id = 1; }",
        },
        // 10. kind_scalar_to_enum: field type changes from scalar to enum
        CompatCase {
            name: "kind_scalar_to_enum",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nenum E { A = 0; }\nmessage U { E id = 1; }",
        },
        // 11. named_type_changed: field references a different named message type
        CompatCase {
            name: "named_type_changed",
            writer: "syntax = \"proto3\";\nmessage A {}\nmessage B {}\nmessage U { A f = 1; }",
            reader: "syntax = \"proto3\";\nmessage A {}\nmessage B {}\nmessage U { B f = 1; }",
        },
        // 12. label_singular_repeat: singular -> repeated
        CompatCase {
            name: "label_singular_repeat",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { repeated int32 id = 1; }",
        },
        // 13. oneof_move_in: move existing fields into a oneof
        CompatCase {
            name: "oneof_move_in",
            writer: "syntax = \"proto3\";\nmessage U { int32 a = 1; int32 b = 2; }",
            reader: "syntax = \"proto3\";\nmessage U { oneof x { int32 a = 1; int32 b = 2; } }",
        },
        // 14. oneof_move_out: move oneof fields back to singular
        CompatCase {
            name: "oneof_move_out",
            writer: "syntax = \"proto3\";\nmessage U { oneof x { int32 a = 1; int32 b = 2; } }",
            reader: "syntax = \"proto3\";\nmessage U { int32 a = 1; int32 b = 2; }",
        },
        // 15. oneof_added: single field moved into a new oneof
        CompatCase {
            name: "oneof_added",
            writer: "syntax = \"proto3\";\nmessage U { int32 a = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { oneof x { int32 a = 1; } }",
        },
        // 16. proto3_optional: add proto3 optional keyword
        CompatCase {
            name: "proto3_optional",
            writer: "syntax = \"proto3\";\nmessage U { int32 a = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { optional int32 a = 1; }",
        },
        // 17. reserved_number: reader adds a reserved field number
        CompatCase {
            name: "reserved_number",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { reserved 2; int32 id = 1; }",
        },
    ]
}

fn protobuf_advanced_cases() -> Vec<CompatCase> {
    vec![
        // 18. reserved_name: reader reserves a field name
        CompatCase {
            name: "reserved_name",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { reserved \"old\"; int32 id = 1; }",
        },
        // 19. map_identical: map field unchanged
        CompatCase {
            name: "map_identical",
            writer: "syntax = \"proto3\";\nmessage U { map<string, int32> m = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { map<string, int32> m = 1; }",
        },
        // 20. map_value_widen: map value type int32 -> int64
        CompatCase {
            name: "map_value_widen",
            writer: "syntax = \"proto3\";\nmessage U { map<string, int32> m = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { map<string, int64> m = 1; }",
        },
        // 21. map_value_to_string: map value type int32 -> string (incompatible)
        CompatCase {
            name: "map_value_to_string",
            writer: "syntax = \"proto3\";\nmessage U { map<string, int32> m = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { map<string, string> m = 1; }",
        },
        // 22. scalar_to_map: plain field -> map field
        CompatCase {
            name: "scalar_to_map",
            writer: "syntax = \"proto3\";\nmessage U { int32 m = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { map<string, int32> m = 1; }",
        },
        // 23. enum_const_added: reader adds a new enum constant
        CompatCase {
            name: "enum_const_added",
            writer: "syntax = \"proto3\";\nenum E { A = 0; }\nmessage U { E e = 1; }",
            reader: "syntax = \"proto3\";\nenum E { A = 0; B = 1; }\nmessage U { E e = 1; }",
        },
        // 24. enum_const_removed: reader removes an enum constant
        CompatCase {
            name: "enum_const_removed",
            writer: "syntax = \"proto3\";\nenum E { A = 0; B = 1; }\nmessage U { E e = 1; }",
            reader: "syntax = \"proto3\";\nenum E { A = 0; }\nmessage U { E e = 1; }",
        },
        // 25. enum_added: reader adds a new standalone enum (unused by message)
        CompatCase {
            name: "enum_added",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nenum E { A = 0; }\nmessage U { int32 id = 1; }",
        },
        // 26. enum_removed: reader removes an unused standalone enum
        CompatCase {
            name: "enum_removed",
            writer: "syntax = \"proto3\";\nenum E { A = 0; }\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
        },
        // 27. message_added: reader adds an additional message definition
        CompatCase {
            name: "message_added",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { int32 id = 1; }\nmessage V { int32 a = 1; }",
        },
        // 28. message_removed: reader removes an extra message definition
        CompatCase {
            name: "message_removed",
            writer: "syntax = \"proto3\";\nmessage U { int32 id = 1; }\nmessage V { int32 a = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { int32 id = 1; }",
        },
        // 29. nested_scalar_change: change type inside nested message
        CompatCase {
            name: "nested_scalar_change",
            writer: "syntax = \"proto3\";\nmessage U { message N { int32 a = 1; } N n = 1; }",
            reader: "syntax = \"proto3\";\nmessage U { message N { string a = 1; } N n = 1; }",
        },
        // 30. package_change: different package declaration
        CompatCase {
            name: "package_change",
            writer: "syntax = \"proto3\";\npackage a;\nmessage U { int32 id = 1; }",
            reader: "syntax = \"proto3\";\npackage b;\nmessage U { int32 id = 1; }",
        },
    ]
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures golden Protobuf compatibility fixtures"]
async fn capture_protobuf_compat_matrix() {
    docker_pull(SR_IMAGE);

    let (broker, _dir) = start_host_broker().await;

    let container_id = docker_run_schema_registry();
    let _guard = ContainerGuard {
        id: container_id.clone(),
    };

    let (matrix, skipped) = run_compat_capture(&container_id).await;

    if !skipped.is_empty() {
        eprintln!(
            "CAPTURE skipped {} cases (cp registration rejected):",
            skipped.len()
        );
        for s in &skipped {
            eprintln!("  SKIPPED: {s}");
        }
    }

    // Minimum sanity: at least 24 cases × 3 levels = 72 entries
    // (allowing up to 6 cases/level-combos to be cp-rejected or to trigger cp 500s).
    assert2::assert!(matrix.len() >= 72);

    let json = serde_json::to_string_pretty(&matrix).expect("serialize matrix");
    write_compat_fixture("protobuf_matrix.json", &json);

    broker.shutdown().await;
    eprintln!(
        "CAPTURE done — protobuf_matrix.json has {} entries ({} cases skipped)",
        matrix.len(),
        skipped.len(),
    );
}

/// Drive the compatibility API and return (result vector, skipped case names).
async fn run_compat_capture(container_id: &str) -> (Vec<serde_json::Value>, Vec<String>) {
    let port = docker_mapped_port(container_id);
    let base = format!("http://127.0.0.1:{port}");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    wait_for_registry(&http, &base, container_id).await;

    let cases = protobuf_cases();
    let levels = ["BACKWARD", "FORWARD", "FULL"];
    let mut results: Vec<serde_json::Value> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();

    for case in &cases {
        for level in levels {
            let subject = format!("{}-{}", case.name, level.to_lowercase());

            // 1. Set compatibility level for this subject.
            set_subject_compat(&http, &base, &subject, level).await;

            // 2. Register writer schema as v1; skip on cp rejection.
            match try_register_writer(&http, &base, &subject, case.writer).await {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("CAPTURE SKIP {subject} (writer registration failed): {e}");
                    skipped.push(format!("{subject}: {e}"));
                    continue;
                }
            }

            // 3. Check if reader is compatible with the registered writer.
            //    cp-schema-registry returns 500 on some oneof transitions (known cp bug); skip.
            let is_compatible = match try_check_compat(&http, &base, &subject, case.reader).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("CAPTURE SKIP {subject} (compat check failed): {e}");
                    skipped.push(format!("{subject}: {e}"));
                    continue;
                }
            };

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

    (results, skipped)
}
