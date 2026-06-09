//! Docker-gated, `#[ignore]` corpus generator. Boots `apache/kafka:4.3.0`,
//! routes real JVM-client traffic through an in-process `kafka-tap`, captures
//! one frame per `(api_key, version, direction)`, then synthesizes the
//! remainder via the JVM oracle. Run manually:
//!   `cargo test -p crabka-protocol --test capture_corpus -- --ignored --nocapture`
mod support;
use support::driver;
#[allow(unused_imports)]
use support::oracle;

use std::collections::BTreeMap;
use std::process::Command;
use std::sync::{Arc, Mutex};

use crabka_kafka_tap::frame::CapturedFrame;
use crabka_kafka_tap::{Recorder, spawn};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/differential_table.rs"
));

/// Captured message bodies keyed by `(api_key, version, is_request)`.
type CaptureMap = Arc<Mutex<BTreeMap<(i16, i16, bool), Vec<u8>>>>;

const IMAGE: &str = "apache/kafka:4.3.0";
const CONTAINER: &str = "crabka-corpus-capture";
const BROKER_HOST_PORT: u16 = 19092;
const TAP_PORT: u16 = 19091;

fn docker_rm_f() {
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER])
        .output();
}

#[allow(clippy::too_many_lines)]
fn docker_run_broker() {
    docker_rm_f();
    let advertised =
        format!("PLAINTEXT://localhost:9092,EXTERNAL://host.docker.internal:{TAP_PORT}");
    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER,
            "--add-host",
            "host.docker.internal:host-gateway",
            "-p",
            &format!("{BROKER_HOST_PORT}:{BROKER_HOST_PORT}"),
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            &format!(
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:{BROKER_HOST_PORT},CONTROLLER://0.0.0.0:9093"
            ),
            "-e",
            &format!("KAFKA_ADVERTISED_LISTENERS={advertised}"),
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
            IMAGE,
        ])
        .output()
        .expect("docker run");
    assert!(
        out.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn tap_upstream() -> String {
    format!("127.0.0.1:{BROKER_HOST_PORT}")
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn wait_ready() {
    for _ in 0..60 {
        let ok = Command::new("docker")
            .args([
                "exec",
                CONTAINER,
                "/opt/kafka/bin/kafka-topics.sh",
                "--list",
                "--bootstrap-server",
                "localhost:9092",
            ])
            .output()
            .is_ok_and(|o| o.status.success());
        if ok {
            return;
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    panic!("broker not ready");
}

#[test]
#[ignore = "requires docker + apache/kafka:4.3.0"]
fn capture_and_generate_corpus() {
    if !docker_available() {
        eprintln!("docker unavailable; skipping");
        return;
    }
    docker_run_broker();
    wait_ready();

    let captured: CaptureMap = Arc::new(Mutex::new(BTreeMap::new()));
    let rec: Recorder = {
        let captured = captured.clone();
        Arc::new(move |f: CapturedFrame| {
            captured
                .lock()
                .unwrap()
                .entry((f.api_key, f.version, f.is_request))
                .or_insert(f.body);
        })
    };
    let addr = spawn(("127.0.0.1", TAP_PORT), &tap_upstream(), rec).unwrap();
    eprintln!("tap on {addr} -> {}", tap_upstream());

    driver::run(CONTAINER);
    std::thread::sleep(std::time::Duration::from_secs(2));

    let pairs = captured.lock().unwrap();
    eprintln!(
        "captured {} distinct (api_key,version,dir) pairs",
        pairs.len()
    );

    // Task 6 inserts post-processing + synthesis here.

    docker_rm_f();
}
