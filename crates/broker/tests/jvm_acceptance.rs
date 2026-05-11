//! End-to-end tests that drive the official Apache Kafka command-line
//! tools (running inside `confluentinc/cp-kafka:6.1.1` via testcontainers)
//! against a Rust `crabka-broker` running on the host.
//!
//! Both tests are gated `#[ignore = "requires Docker"]` so `cargo test`
//! doesn't pull Docker by default. Run with `--ignored`.
//!
//! Networking: the broker binds `0.0.0.0:<port>` on the host. The JVM
//! tools running inside the container reach back via either the Linux
//! Docker bridge gateway (CI sets `CRABKA_HOST_BOOTSTRAP=<bridge_ip>:<port>`)
//! or `host.docker.internal:<port>` on Docker Desktop. The advertised
//! listener must MATCH the bootstrap address (else Metadata redirects
//! the JVM client back to an unreachable address). See `KNOWN_ISSUES.md`.

#![cfg(not(target_os = "windows"))]

use std::process::{Command, Stdio};

use crabka_broker::{Broker, BrokerConfig};
use crabka_log::LogConfig;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::Kafka;

const DEFAULT_HOST_PORT: u16 = 9092;

/// Determine the host port we should bind to, and what the in-container
/// JVM tool should use as `--bootstrap-server`.
///
/// If `CRABKA_HOST_BOOTSTRAP=host:port` is set, parse the port from it
/// and bind to that exact port — CI relies on this. Otherwise default
/// to `host.docker.internal:9092` (Docker Desktop convention).
fn resolve_host_bootstrap() -> (String, u16) {
    if let Ok(env_value) = std::env::var("CRABKA_HOST_BOOTSTRAP") {
        let port = env_value
            .rsplit(':')
            .next()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(DEFAULT_HOST_PORT);
        (env_value, port)
    } else {
        (
            format!("host.docker.internal:{DEFAULT_HOST_PORT}"),
            DEFAULT_HOST_PORT,
        )
    }
}

/// Spawn the broker, listening on `0.0.0.0:<port>` so the in-container
/// JVM client can reach it via the Docker bridge gateway (Linux) or
/// `host.docker.internal` (Docker Desktop).
///
/// `advertised_listener` is set to the same `host:port` the JVM client
/// uses in `--bootstrap-server` — Metadata responses tell the JVM client
/// where to go, so this must be reachable from inside the container.
async fn start_host_broker() -> (
    crabka_broker::BrokerHandle,
    String,
    tempfile::TempDir,
) {
    let (advertised, port) = resolve_host_bootstrap();
    let dir = tempfile::tempdir().unwrap();
    let listen_addr = format!("0.0.0.0:{port}").parse().unwrap();
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: advertised.clone(),
        log_dir: dir.path().to_path_buf(),
        log_config: LogConfig::default(),
    };
    let handle = Broker::start(config).await.expect("start broker");
    (handle, advertised, dir)
}

/// Run `docker exec <container_id> <args...>`, asserting success.
fn docker_exec(container_id: &str, args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .arg("exec")
        .arg(container_id)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn docker exec");
    assert!(
        out.status.success(),
        "docker exec {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn console_producer_round_trip() {
    const TOPIC: &str = "crabka-broker-itest";

    // 1. Start a kafka container — only needed for the command-line binaries.
    //    The broker we're testing is our Rust process on the host.
    let cp_kafka: ContainerAsync<Kafka> = Kafka::default().start().await.unwrap();
    let container_id = cp_kafka.id().to_string();

    let (broker, bootstrap, _dir) = start_host_broker().await;

    // 2. Create the topic via the JVM client.
    docker_exec(
        &container_id,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            &bootstrap,
        ],
    );

    // 3. Produce 3 records via stdin.
    let mut child = Command::new("docker")
        .args([
            "exec",
            "-i",
            &container_id,
            "kafka-console-producer",
            "--bootstrap-server",
            &bootstrap,
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    use std::io::Write;
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .unwrap();
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // 4. Consume them back via --partition 0 (bypasses groups entirely).
    let consumer_out = Command::new("docker")
        .args([
            "exec",
            &container_id,
            "kafka-console-consumer",
            "--bootstrap-server",
            &bootstrap,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "10000",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn consumer");
    assert!(
        consumer_out.status.success(),
        "consumer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&consumer_out.stdout),
        String::from_utf8_lossy(&consumer_out.stderr),
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(s.contains(needle), "consumer didn't emit {needle}: {s:?}");
    }

    broker.shutdown().await;
    let _ = cp_kafka.stop().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_topics_describe_smokes_metadata() {
    let cp_kafka: ContainerAsync<Kafka> = Kafka::default().start().await.unwrap();
    let container_id = cp_kafka.id().to_string();
    let (broker, bootstrap, _dir) = start_host_broker().await;

    docker_exec(
        &container_id,
        &[
            "kafka-topics",
            "--create",
            "--topic",
            "described",
            "--partitions",
            "2",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            &bootstrap,
        ],
    );

    let out = docker_exec(
        &container_id,
        &[
            "kafka-topics",
            "--describe",
            "--topic",
            "described",
            "--bootstrap-server",
            &bootstrap,
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Topic: described"),
        "describe missing topic line: {stdout}"
    );
    assert!(
        stdout.contains("PartitionCount: 2"),
        "describe missing partition count: {stdout}"
    );

    broker.shutdown().await;
    let _ = cp_kafka.stop().await;
}
