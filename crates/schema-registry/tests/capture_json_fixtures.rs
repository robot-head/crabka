//! Golden compatibility-verdict capture harness for Crabka Schema Registry slice 2c.
//!
//! Boots a real `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0` container against an
//! in-process Crabka broker, then drives the compatibility check API for ~47
//! JSON Schema cases × 3 compatibility levels ≈ 141 entries. Verdicts are written to:
//!
//!   `tests/fixtures/compat/json_matrix.json`
//!
//! That file is the oracle for Task 6's JSON Schema compatibility engine calibration.
//!
//! ```text
//! cargo test -p crabka-schema-registry --test capture_json_fixtures -- --ignored --nocapture
//! ```
//!
//! Re-running this test regenerates the fixture file verbatim.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use crabka_broker::{Broker, BrokerConfig, NodeId};

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
        node_id: NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(NodeId(1), controller_addr.to_string())],
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

/// POST /subjects/{subject}/versions to register writer as v1 with JSON schema type.
/// Returns `Ok(())` on success, `Err(error_text)` if cp rejects it (caller logs + skips).
async fn try_register_writer(
    http: &reqwest::Client,
    base: &str,
    subject: &str,
    schema: &str,
) -> Result<(), String> {
    let url = format!("{base}/subjects/{subject}/versions");
    let body = serde_json::json!({ "schema": schema, "schemaType": "JSON" });
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
/// `Err(error_text)` if cp returns a non-success status (caller logs + skips).
async fn try_check_compat(
    http: &reqwest::Client,
    base: &str,
    subject: &str,
    reader: &str,
) -> Result<bool, String> {
    let url = format!("{base}/compatibility/subjects/{subject}/versions/latest");
    let body = serde_json::json!({ "schema": reader, "schemaType": "JSON" });
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

// ── JSON Schema case definitions ──────────────────────────────────────────────

struct CompatCase {
    name: &'static str,
    writer: &'static str,
    reader: &'static str,
}

fn json_schema_cases() -> Vec<CompatCase> {
    let mut cases = json_schema_basic_cases();
    cases.extend(json_schema_constraint_cases());
    cases.extend(json_schema_composition_cases());
    cases
}

fn json_schema_basic_cases() -> Vec<CompatCase> {
    vec![
        // 1. add_prop_open: reader adds a new property (open schema, no additionalProperties:false)
        CompatCase {
            name: "add_prop_open",
            writer: r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#,
            reader: r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#,
        },
        // 2. remove_prop_open: reader removes a property (open schema) — reverse of add_prop_open
        CompatCase {
            name: "remove_prop_open",
            writer: r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#,
            reader: r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#,
        },
        // 3. add_prop_closed: reader adds a new property with additionalProperties:false
        CompatCase {
            name: "add_prop_closed",
            writer: r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}"#,
            reader: r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#,
        },
        // 4. remove_prop_closed: reader removes a property with additionalProperties:false — reverse
        CompatCase {
            name: "remove_prop_closed",
            writer: r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#,
            reader: r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}"#,
        },
        // 5. required_added: reader adds a required constraint
        CompatCase {
            name: "required_added",
            writer: r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#,
            reader: r#"{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"]}"#,
        },
        // 6. required_removed: reader removes a required constraint — reverse
        CompatCase {
            name: "required_removed",
            writer: r#"{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"]}"#,
            reader: r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#,
        },
        // 7. addl_false_to_true: reader relaxes additionalProperties from false to true
        CompatCase {
            name: "addl_false_to_true",
            writer: r#"{"type":"object","additionalProperties":false}"#,
            reader: r#"{"type":"object","additionalProperties":true}"#,
        },
        // 8. addl_true_to_false: reader tightens additionalProperties from true to false — reverse
        CompatCase {
            name: "addl_true_to_false",
            writer: r#"{"type":"object","additionalProperties":true}"#,
            reader: r#"{"type":"object","additionalProperties":false}"#,
        },
        // 9. type_widen: reader accepts null in addition to string
        CompatCase {
            name: "type_widen",
            writer: r#"{"type":"string"}"#,
            reader: r#"{"type":["string","null"]}"#,
        },
        // 10. type_narrow: reader drops null from string|null — reverse
        CompatCase {
            name: "type_narrow",
            writer: r#"{"type":["string","null"]}"#,
            reader: r#"{"type":"string"}"#,
        },
        // 11. type_changed: reader changes string to integer (incompatible)
        CompatCase {
            name: "type_changed",
            writer: r#"{"type":"string"}"#,
            reader: r#"{"type":"integer"}"#,
        },
        // 12. enum_extended: reader adds a new enum value
        CompatCase {
            name: "enum_extended",
            writer: r#"{"enum":["a"]}"#,
            reader: r#"{"enum":["a","b"]}"#,
        },
        // 13. enum_narrowed: reader removes an enum value — reverse
        CompatCase {
            name: "enum_narrowed",
            writer: r#"{"enum":["a","b"]}"#,
            reader: r#"{"enum":["a"]}"#,
        },
        // 14. maximum_added: reader adds a maximum constraint
        CompatCase {
            name: "maximum_added",
            writer: r#"{"type":"integer"}"#,
            reader: r#"{"type":"integer","maximum":10}"#,
        },
        // 15. maximum_removed: reader removes a maximum constraint — reverse
        CompatCase {
            name: "maximum_removed",
            writer: r#"{"type":"integer","maximum":10}"#,
            reader: r#"{"type":"integer"}"#,
        },
        // 16. maximum_increased: reader relaxes maximum from 10 to 100
        CompatCase {
            name: "maximum_increased",
            writer: r#"{"type":"integer","maximum":10}"#,
            reader: r#"{"type":"integer","maximum":100}"#,
        },
        // 17. maximum_decreased: reader tightens maximum from 100 to 10 — reverse
        CompatCase {
            name: "maximum_decreased",
            writer: r#"{"type":"integer","maximum":100}"#,
            reader: r#"{"type":"integer","maximum":10}"#,
        },
    ]
}

fn json_schema_constraint_cases() -> Vec<CompatCase> {
    vec![
        // 18. minimum_added: reader adds a minimum constraint
        CompatCase {
            name: "minimum_added",
            writer: r#"{"type":"integer"}"#,
            reader: r#"{"type":"integer","minimum":1}"#,
        },
        // 19. minimum_removed: reader removes a minimum constraint — reverse
        CompatCase {
            name: "minimum_removed",
            writer: r#"{"type":"integer","minimum":1}"#,
            reader: r#"{"type":"integer"}"#,
        },
        // 20. exclusive_max_added: reader adds an exclusiveMaximum constraint
        CompatCase {
            name: "exclusive_max_added",
            writer: r#"{"type":"integer"}"#,
            reader: r#"{"type":"integer","exclusiveMaximum":10}"#,
        },
        // 21. multiple_of_added: reader adds a multipleOf constraint
        CompatCase {
            name: "multiple_of_added",
            writer: r#"{"type":"integer"}"#,
            reader: r#"{"type":"integer","multipleOf":5}"#,
        },
        // 22. min_length_added: reader adds a minLength constraint
        CompatCase {
            name: "min_length_added",
            writer: r#"{"type":"string"}"#,
            reader: r#"{"type":"string","minLength":3}"#,
        },
        // 23. min_length_removed: reader removes a minLength constraint — reverse
        CompatCase {
            name: "min_length_removed",
            writer: r#"{"type":"string","minLength":3}"#,
            reader: r#"{"type":"string"}"#,
        },
        // 24. max_length_added: reader adds a maxLength constraint
        CompatCase {
            name: "max_length_added",
            writer: r#"{"type":"string"}"#,
            reader: r#"{"type":"string","maxLength":9}"#,
        },
        // 25. pattern_added: reader adds a pattern constraint
        CompatCase {
            name: "pattern_added",
            writer: r#"{"type":"string"}"#,
            reader: r#"{"type":"string","pattern":"^x"}"#,
        },
        // 26. min_items_added: reader adds a minItems constraint
        CompatCase {
            name: "min_items_added",
            writer: r#"{"type":"array"}"#,
            reader: r#"{"type":"array","minItems":1}"#,
        },
        // 27. max_items_added: reader adds a maxItems constraint
        CompatCase {
            name: "max_items_added",
            writer: r#"{"type":"array"}"#,
            reader: r#"{"type":"array","maxItems":5}"#,
        },
    ]
}

fn json_schema_composition_cases() -> Vec<CompatCase> {
    vec![
        // 28. items_type_change: reader changes array items type from integer to string
        CompatCase {
            name: "items_type_change",
            writer: r#"{"type":"array","items":{"type":"integer"}}"#,
            reader: r#"{"type":"array","items":{"type":"string"}}"#,
        },
        // 29. min_properties_added: reader adds a minProperties constraint
        CompatCase {
            name: "min_properties_added",
            writer: r#"{"type":"object"}"#,
            reader: r#"{"type":"object","minProperties":1}"#,
        },
        // 30. anyof_subschema_added: reader adds a branch to anyOf
        CompatCase {
            name: "anyof_subschema_added",
            writer: r#"{"anyOf":[{"type":"string"}]}"#,
            reader: r#"{"anyOf":[{"type":"string"},{"type":"integer"}]}"#,
        },
        // 31. anyof_subschema_removed: reader removes a branch from anyOf — reverse
        CompatCase {
            name: "anyof_subschema_removed",
            writer: r#"{"anyOf":[{"type":"string"},{"type":"integer"}]}"#,
            reader: r#"{"anyOf":[{"type":"string"}]}"#,
        },
        // 32. allof_subschema_added: reader adds a subschema to allOf (more restrictive)
        CompatCase {
            name: "allof_subschema_added",
            writer: r#"{"allOf":[{"type":"object"}]}"#,
            reader: r#"{"allOf":[{"type":"object"},{"type":"object","required":["a"]}]}"#,
        },
        // 33. oneof_subschema_added: reader adds a branch to oneOf
        CompatCase {
            name: "oneof_subschema_added",
            writer: r#"{"oneOf":[{"type":"string"}]}"#,
            reader: r#"{"oneOf":[{"type":"string"},{"type":"integer"}]}"#,
        },
        // 34. not_added: reader adds a not constraint
        CompatCase {
            name: "not_added",
            writer: r#"{"type":"object"}"#,
            reader: r#"{"not":{"type":"string"}}"#,
        },
        // 35. ref_target_type_change: $ref target changes type from integer to string
        CompatCase {
            name: "ref_target_type_change",
            writer: r##"{"$ref":"#/$defs/T","$defs":{"T":{"type":"integer"}}}"##,
            reader: r##"{"$ref":"#/$defs/T","$defs":{"T":{"type":"string"}}}"##,
        },
        // 36. dependency_added: reader adds a dependentRequired constraint
        CompatCase {
            name: "dependency_added",
            writer: r#"{"type":"object"}"#,
            reader: r#"{"type":"object","dependentRequired":{"a":["b"]}}"#,
        },
        // 37. if_then_added: reader adds if/then conditional keywords
        CompatCase {
            name: "if_then_added",
            writer: r#"{"type":"object"}"#,
            reader: r#"{"type":"object","if":{"required":["a"]},"then":{"required":["b"]}}"#,
        },
    ]
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures golden JSON Schema compatibility fixtures"]
async fn capture_json_compat_matrix() {
    docker_pull(SR_IMAGE);

    let (broker, _dir) = start_host_broker().await;

    let container_id = docker_run_schema_registry();
    let _guard = ContainerGuard {
        id: container_id.clone(),
    };

    let (matrix, skipped) = run_compat_capture(&container_id).await;

    if !skipped.is_empty() {
        eprintln!(
            "CAPTURE skipped {} cases (cp registration or compat check failed):",
            skipped.len()
        );
        for s in &skipped {
            eprintln!("  SKIPPED: {s}");
        }
    }

    // Minimum sanity: at least 30 cases × 3 levels = 90 entries
    // (allowing up to 7 cases/level-combos to be cp-rejected or return errors).
    assert2::assert!(matrix.len() >= 90);

    let json = serde_json::to_string_pretty(&matrix).expect("serialize matrix");
    write_compat_fixture("json_matrix.json", &json);

    broker.shutdown().await;
    eprintln!(
        "CAPTURE done — json_matrix.json has {} entries ({} cases skipped)",
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

    let cases = json_schema_cases();
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
            //    cp-schema-registry may return 422/500 on certain JSON Schema constructs; skip.
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
