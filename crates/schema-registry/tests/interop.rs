//! Docker interop test: prove our `KafkaStore` + `StoreReader` can decode
//! `_schemas` records that a REAL `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0`
//! wrote, and that our REST router returns the same schema through `GET`.
//!
//! Mirrors the setup in `capture_fixtures.rs`:
//! - A Crabka broker binds `0.0.0.0:9092`, advertises `host.docker.internal:9092`.
//! - cp-schema-registry connects to it via Docker's `--add-host` gateway.
//! - We register an Avro schema through cp's REST endpoint.
//! - Then we start OUR `KafkaStore` (which replays the `_schemas` topic that cp wrote)
//!   and assert `GET /schemas/ids/1` returns the schema, and `GET /subjects` lists it.
//!
//! Gated `#[ignore]` so `cargo test --workspace` never needs Docker. Run with:
//!
//! ```text
//! cargo test -p crabka-schema-registry --test interop -- --ignored --nocapture
//! ```
//!
//! The test tears down the container on both success and failure, through
//! `ContainerGuard`.

use std::{
    net::SocketAddr,
    process::Command,
    time::{Duration, Instant},
};

use axum::{body::Body, http::Request};
use crabka_broker::{Broker, BrokerConfig};
use crabka_schema_registry::{
    config::{RegistryConfig, SecurityConfig},
    kafkastore::KafkaStore,
    rest::{self, AppState},
};
use crabka_units::prelude::*;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

// ── network constants (mirrors capture_fixtures.rs) ──────────────────────────

const LISTEN: &str = "0.0.0.0:9092";
const CONTROLLER_LISTEN: &str = "0.0.0.0:9093";
const ADVERTISED: &str = "host.docker.internal:9092";
const DIRECT_ADDR: &str = "127.0.0.1:9092";

const SR_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0";
const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

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
    let listen_addr: SocketAddr = LISTEN.parse().expect("static listen addr");
    let controller_addr: SocketAddr = CONTROLLER_LISTEN.parse().expect("static controller addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: ADVERTISED.into(),
        log_dir: dir.path().to_path_buf(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: secs(3),
        heartbeat_timeout: secs(9),
        replica_lag_time_max: secs(30),
        controller_election_timeout: secs(5),
        controller_heartbeat_interval: millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!("INTEROP broker started listen={LISTEN} advertised={ADVERTISED}");
    (handle, dir)
}

// ── docker helpers ─────────────────────────────────────────────────────────────

fn docker_pull(image: &str) {
    eprintln!("INTEROP docker pull {image}...");
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
    eprintln!("INTEROP container id={id}");
    id
}

fn docker_mapped_port(id: &str) -> u16 {
    let out = Command::new("docker")
        .args(["port", id, "8081"])
        .output()
        .expect("spawn docker port");
    assert2::assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    text.lines()
        .filter_map(|l| l.rsplit(':').next())
        .find_map(|p| p.trim().parse::<u16>().ok())
        .unwrap_or_else(|| panic!("could not parse mapped 8081 port from: {text:?}"))
}

fn docker_logs(id: &str) -> String {
    let out = Command::new("docker")
        .args(["logs", id])
        .output()
        .expect("spawn docker logs");
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn docker_rm_f(id: &str) {
    let _ = Command::new("docker").args(["rm", "-f", id]).output();
    eprintln!("INTEROP removed container {id}");
}

struct ContainerGuard {
    id: String,
}
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        docker_rm_f(&self.id);
    }
}

// ── REST helpers ───────────────────────────────────────────────────────────────

/// Poll `GET /subjects` until 200 or 120s.
async fn wait_for_registry(http: &reqwest::Client, base: &str, container_id: &str) {
    let deadline = Instant::now() + Duration::from_mins(2);
    let url = format!("{base}/subjects");
    let mut last: Option<String> = None;
    while Instant::now() < deadline {
        match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                eprintln!("INTEROP schema-registry READY ({})", resp.status());
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

/// POST a schema registration to cp-schema-registry's REST endpoint.
async fn register_via_cp(http: &reqwest::Client, base: &str, subject: &str, schema: &str) -> i64 {
    let body = serde_json::json!({ "schema": schema });
    let url = format!("{base}/subjects/{subject}/versions");
    let resp = http
        .post(&url)
        .header("Content-Type", SR_CONTENT_TYPE)
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {url}: {e}"));
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert2::assert!(status.is_success());
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    v["id"]
        .as_i64()
        .unwrap_or_else(|| panic!("no id in {text}"))
}

// ── our router helpers ─────────────────────────────────────────────────────────

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let b = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    serde_json::from_slice(&b).unwrap()
}

async fn get_json(app: &axum::Router, uri: &str) -> serde_json::Value {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    body_json(resp).await
}

// ── the test ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn our_store_decodes_cp_schema_registry_records() {
    let avro_schema = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"#;

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

    // Register one Avro schema via the REAL cp-schema-registry.
    let id = register_via_cp(&http, &base, "av-value", avro_schema).await;
    eprintln!("INTEROP cp registered id={id}");
    assert2::assert!(id == 1);

    // Brief pause so the `_schemas` topic record is durable before our
    // KafkaStore starts its reader.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now start OUR KafkaStore against the SAME broker (direct 127.0.0.1).
    let cfg = RegistryConfig {
        bootstrap: DIRECT_ADDR.to_string(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: "sr-interop".into(),
        advertised_url: "http://127.0.0.1:0".into(),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        runtime: crabka_schema_registry::config::RegistryRuntimeConfig::default(),
        security: SecurityConfig::default(),
    };
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone())
        .await
        .expect("start KafkaStore");

    // Give the reader a moment to replay the existing records.
    // The reader is live and will have caught up by the time we poll.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let subjects = store.store.read().subjects(false);
        if subjects.contains(&"av-value".to_string()) {
            eprintln!("INTEROP store has av-value after replay");
            break;
        }
        assert2::assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Build our REST router on top of the replayed store.
    let app = rest::router(AppState { store });

    // Assert GET /schemas/ids/1 returns the schema cp registered.
    let got = get_json(&app, "/schemas/ids/1").await;
    eprintln!("INTEROP GET /schemas/ids/1 = {got}");
    let schema_type_omitted = got.get("schemaType").is_none();
    let schema_str = got["schema"].as_str().expect("schema field is a string");
    // Parse both sides as JSON and compare structurally (field order may differ).
    let got_v: serde_json::Value = serde_json::from_str(schema_str)
        .unwrap_or_else(|e| panic!("schema is not valid JSON: {e}\n  raw: {schema_str}"));
    let expected_v: serde_json::Value = serde_json::from_str(avro_schema).unwrap();
    assert2::assert!(schema_type_omitted);
    assert2::assert!(got_v == expected_v);

    // Assert GET /subjects lists "av-value".
    let subs = get_json(&app, "/subjects").await;
    eprintln!("INTEROP GET /subjects = {subs}");
    let names: Vec<String> = subs
        .as_array()
        .expect("subjects is an array")
        .iter()
        .map(|v| v.as_str().expect("string").to_string())
        .collect();
    assert2::assert!(names.contains(&"av-value".to_string()));

    eprintln!(
        "INTEROP PASS: our StoreReader successfully decoded cp-schema-registry's _schemas records"
    );

    cancel.cancel();
    broker.shutdown().await;
}
