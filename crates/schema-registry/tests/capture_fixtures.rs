//! Golden-fixture capture harness for the Crabka Schema Registry slice.
//!
//! This test stands up a **real** `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0`
//! container pointed at an in-process Crabka broker, registers a handful of
//! AVRO / PROTOBUF / JSON schemas through the official REST API, and captures
//! the byte-exact REST responses **and** the raw `_schemas` Kafka log records
//! that cp-schema-registry writes. Those captures become the byte-exact oracle
//! the later schema-registry tasks are validated against, so accuracy matters
//! more than speed.
//!
//! Networking mirrors `crates/broker/tests/jvm_acceptance.rs`: the Crabka
//! broker listens on `0.0.0.0:9092` and advertises `host.docker.internal:9092`.
//! The container is launched with `--add-host=host.docker.internal:host-gateway`
//! so the JVM Kafka client inside it can reach the host broker, while the
//! host-side raw `_schemas` read connects **directly** to `127.0.0.1:9092`
//! (a host process can't resolve `host.docker.internal`).
//!
//! Gated `#[ignore]` so `cargo test --workspace` never pulls Docker. Run with:
//!
//! ```text
//! cargo test -p crabka-schema-registry --test capture_fixtures -- --ignored --nocapture
//! ```
//!
//! The fixtures it writes live under `tests/fixtures/`; re-running this test
//! regenerates them verbatim.

// Match the convention used by the broker integration tests: this is a
// Docker-driven capture harness, not production code, so the pedantic group is
// allowed wholesale (e.g. `doc_markdown`, `redundant_closure_for_method_calls`,
// `duration_suboptimal_units`).
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use crabka_broker::{Broker, BrokerConfig, NodeId};
use crabka_client_admin::AdminClient;
use crabka_client_core::{Connection, ConnectionOptions, fetch_partition};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

/// The broker binds host port 9092 (embedded in [`LISTEN`]) and
/// cp-schema-registry's Kafka client reaches it via `host.docker.internal:9092`
/// (embedded in [`ADVERTISED`]).
const LISTEN: &str = "0.0.0.0:9092";
const CONTROLLER_LISTEN: &str = "0.0.0.0:9093";
/// Advertised listener: a name the container can resolve through
/// `--add-host=host.docker.internal:host-gateway`.
const ADVERTISED: &str = "host.docker.internal:9092";
/// Host-side direct address for the raw `_schemas` read. We connect here and
/// fetch on THIS connection so we never follow the advertised
/// `host.docker.internal` address (unresolvable from a host process).
const DIRECT_ADDR: &str = "127.0.0.1:9092";

const SR_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0";

/// Content type cp-schema-registry expects on register POSTs.
const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

// ── fixture directory ────────────────────────────────────────────────────────

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

/// Write `body` verbatim to `tests/fixtures/<name>`.
fn write_fixture(name: &str, body: &str) {
    let path = fixtures_dir().join(name);
    std::fs::write(&path, body).unwrap_or_else(|e| panic!("write fixture {}: {e}", path.display()));
    eprintln!("CAPTURE wrote {} ({} bytes)", path.display(), body.len());
}

// ── broker ───────────────────────────────────────────────────────────────────

/// Spawn an in-process Crabka broker on `0.0.0.0:9092` advertising
/// `host.docker.internal:9092`, mirroring `jvm_acceptance.rs`.
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

// ── docker helpers ───────────────────────────────────────────────────────────

/// `docker pull <image>`, allowing several minutes for the large SR image.
fn docker_pull(image: &str) {
    eprintln!("CAPTURE docker pull {image} (large; may take minutes)...");
    let out = Command::new("docker")
        .args(["pull", image])
        .output()
        .expect("spawn docker pull");
    assert2::assert!(out.status.success());
}

/// `docker run -d` cp-schema-registry pointed at the host broker via
/// `host.docker.internal`. Returns the container id (trimmed).
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

/// Resolve the host port Docker mapped to container port 8081.
fn docker_mapped_port(id: &str) -> u16 {
    let out = Command::new("docker")
        .args(["port", id, "8081"])
        .output()
        .expect("spawn docker port");
    assert2::assert!(out.status.success());
    // Output lines look like `0.0.0.0:54321` (and possibly an IPv6 line).
    let text = String::from_utf8_lossy(&out.stdout);
    let port = text
        .lines()
        .filter_map(|l| l.rsplit(':').next())
        .find_map(|p| p.trim().parse::<u16>().ok())
        .unwrap_or_else(|| panic!("could not parse mapped 8081 port from: {text:?}"));
    eprintln!("CAPTURE schema-registry mapped 8081 -> host {port}");
    port
}

/// Dump container logs (best-effort) for debugging on failure.
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

/// RAII guard: force-removes the container on drop, so an assertion failure
/// (panic) anywhere in the capture body never leaks a running container.
struct ContainerGuard {
    id: String,
}
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        docker_rm_f(&self.id);
    }
}

// ── REST helpers ─────────────────────────────────────────────────────────────

/// Poll `GET /subjects` until 200 or timeout. Returns once ready.
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
        "schema-registry never became ready within 120s (last: {last:?}).\n\
         container logs:\n{logs}"
    );
}

/// Register a schema, returning the verbatim response body. `schema_type` is
/// `None` for AVRO (the field is omitted, matching the SR default).
async fn register_schema(
    http: &reqwest::Client,
    base: &str,
    subject: &str,
    schema: &str,
    schema_type: Option<&str>,
) -> String {
    let body = match schema_type {
        Some(t) => serde_json::json!({ "schema": schema, "schemaType": t }),
        None => serde_json::json!({ "schema": schema }),
    };
    let url = format!("{base}/subjects/{subject}/versions");
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
    assert2::assert!(status.is_success());
    text
}

/// `GET url`, returning `(status, verbatim body)`.
async fn http_get(http: &reqwest::Client, url: &str) -> (reqwest::StatusCode, String) {
    let resp = http
        .get(url)
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {url} failed: {e}"));
    let status = resp.status();
    let text = resp
        .text()
        .await
        .unwrap_or_else(|e| panic!("read body of GET {url}: {e}"));
    (status, text)
}

/// Extract the integer `id` field from a register/get response body.
fn extract_id(body: &str) -> i64 {
    let v: serde_json::Value =
        serde_json::from_str(body).unwrap_or_else(|e| panic!("parse id from {body}: {e}"));
    v.get("id")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("no integer `id` in {body}"))
}

/// Wrap an error capture as `{"_http_status": N, "_body": "<raw>"}`.
fn error_fixture_json(status: reqwest::StatusCode, raw_body: &str) -> String {
    // Store the raw body as a JSON string (escaped) so the fixture is itself
    // valid JSON while preserving the exact bytes SR returned.
    let v = serde_json::json!({
        "_http_status": status.as_u16(),
        "_body": raw_body,
    });
    serde_json::to_string_pretty(&v).expect("serialize error fixture")
}

// ── raw _schemas read ────────────────────────────────────────────────────────

/// Resolve the `_schemas` topic id via `AdminClient` on the direct host address.
async fn resolve_schemas_topic_id() -> WireUuid {
    let mut admin = AdminClient::connect(&[DIRECT_ADDR.to_string()])
        .await
        .expect("admin connect");
    // Retry: the `_schemas` topic is created lazily by SR's first write.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let md = admin.metadata(&["_schemas"]).await.expect("metadata");
        if let Some(entry) = md.topics.iter().find(|t| t.name == "_schemas")
            && let Some(id) = entry.topic_id
        {
            let wire = WireUuid(id.into_bytes());
            if wire != WireUuid::ZERO {
                eprintln!("CAPTURE _schemas topic_id={id}");
                return wire;
            }
        }
        assert2::assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Read every record from `_schemas` partition 0 (offset 0..) over a dedicated
/// direct connection, writing one fixture per record in offset order.
async fn capture_schemas_log(topic_id: WireUuid) {
    let addr: SocketAddr = DIRECT_ADDR.parse().expect("direct addr");
    let conn = Connection::connect_with_options(addr, ConnectionOptions::default())
        .await
        .expect("direct connect for _schemas read");

    let mut next_offset: i64 = 0;
    let mut idx: usize = 0;
    // Loop fetching until a fetch returns no new records (the writes are done
    // by the time we read, so a single empty fetch means we've drained).
    loop {
        let records = fetch_partition(&conn, "_schemas", topic_id, 0, next_offset, 1000, 1 << 20)
            .await
            .expect("fetch _schemas");
        if records.is_empty() {
            break;
        }
        for r in &records {
            if r.offset < next_offset {
                continue;
            }
            let key = r
                .key
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned());
            let value = r
                .value
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned());
            // Build the wrapper by hand so the inner key/value JSON is embedded
            // verbatim (as a JSON string) rather than re-parsed/re-serialized.
            let wrapper = serde_json::json!({ "key": key, "value": value });
            write_fixture(
                &format!("schemas_record_{idx}.json"),
                &serde_json::to_string_pretty(&wrapper).expect("serialize record wrapper"),
            );
            eprintln!(
                "CAPTURE _schemas[{}] key={:?} value={:?}",
                r.offset, key, value
            );
            next_offset = r.offset + 1;
            idx += 1;
        }
    }
    conn.close();
    eprintln!("CAPTURE captured {idx} _schemas records");
}

// ── the test ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures golden fixtures"]
async fn capture_golden_fixtures() {
    // Schemas under test.
    let avro_schema = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"#;
    let protobuf_schema = "syntax = \"proto3\"; message User { int32 id = 1; }";
    let json_schema = r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#;

    docker_pull(SR_IMAGE);

    let (broker, _dir) = start_host_broker().await;

    let container_id = docker_run_schema_registry();
    // RAII teardown: removes the container even if the capture body panics.
    let _guard = ContainerGuard {
        id: container_id.clone(),
    };

    run_capture(&container_id, avro_schema, protobuf_schema, json_schema).await;

    // Drain the broker cleanly on the success path. On a panic the guard above
    // still tears the container down; the broker's in-process tasks are dropped
    // with the test process.
    broker.shutdown().await;
    eprintln!("CAPTURE done");
}

/// The capture body, factored out so the caller can ensure container teardown.
async fn run_capture(
    container_id: &str,
    avro_schema: &str,
    protobuf_schema: &str,
    json_schema: &str,
) {
    let port = docker_mapped_port(container_id);
    let base = format!("http://127.0.0.1:{port}");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    wait_for_registry(&http, &base, container_id).await;

    // ── register the three schemas ──
    let avro_reg = register_schema(&http, &base, "av-value", avro_schema, None).await;
    write_fixture("rest_register_avro.json", &avro_reg);

    let pb_reg = register_schema(&http, &base, "pb-value", protobuf_schema, Some("PROTOBUF")).await;
    write_fixture("rest_register_protobuf.json", &pb_reg);

    let js_reg = register_schema(&http, &base, "js-value", json_schema, Some("JSON")).await;
    write_fixture("rest_register_json.json", &js_reg);

    let avro_id = extract_id(&avro_reg);
    let pb_id = extract_id(&pb_reg);
    let js_id = extract_id(&js_reg);
    eprintln!("CAPTURE ids avro={avro_id} protobuf={pb_id} json={js_id}");

    // ── GET version + by-id for each, then misc GETs ──
    for (url, fixture, _what) in [
        (
            format!("{base}/subjects/av-value/versions/1"),
            "rest_get_version_avro.json",
            "get av-value v1",
        ),
        (
            format!("{base}/schemas/ids/{avro_id}"),
            "rest_get_by_id_avro.json",
            "get avro by id",
        ),
        (
            format!("{base}/subjects/pb-value/versions/1"),
            "rest_get_version_protobuf.json",
            "get pb-value v1",
        ),
        (
            format!("{base}/schemas/ids/{pb_id}"),
            "rest_get_by_id_protobuf.json",
            "get protobuf by id",
        ),
        (
            format!("{base}/subjects/js-value/versions/1"),
            "rest_get_version_json.json",
            "get js-value v1",
        ),
        (
            format!("{base}/schemas/ids/{js_id}"),
            "rest_get_by_id_json.json",
            "get json by id",
        ),
        (
            format!("{base}/subjects"),
            "rest_list_subjects.json",
            "list subjects",
        ),
        (
            format!("{base}/config"),
            "rest_get_config.json",
            "get config",
        ),
        (
            format!("{base}/schemas/types"),
            "rest_schema_types.json",
            "schema types",
        ),
    ] {
        let (st, body) = http_get(&http, &url).await;
        assert2::assert!(st.is_success());
        write_fixture(fixture, &body);
    }

    // ── provoke + capture errors (status + raw body) ──
    let (st, body) = http_get(&http, &format!("{base}/subjects/does-not-exist/versions/1")).await;
    eprintln!("CAPTURE err subject_not_found status={st} body={body}");
    write_fixture(
        "rest_err_subject_not_found.json",
        &error_fixture_json(st, &body),
    );

    // Invalid schema: malformed AVRO body.
    let bad_resp = http
        .post(format!("{base}/subjects/bad-value/versions"))
        .header("Content-Type", SR_CONTENT_TYPE)
        .body(r#"{"schema":"{ this is not valid avro"}"#)
        .send()
        .await
        .expect("POST bad schema");
    let bad_status = bad_resp.status();
    let bad_body = bad_resp.text().await.expect("read bad schema body");
    eprintln!("CAPTURE err invalid_schema status={bad_status} body={bad_body}");
    write_fixture(
        "rest_err_invalid_schema.json",
        &error_fixture_json(bad_status, &bad_body),
    );

    // ── raw _schemas log capture ──
    let topic_id = resolve_schemas_topic_id().await;
    capture_schemas_log(topic_id).await;
}
