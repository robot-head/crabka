//! End-to-end tests that drive the official Apache Kafka command-line
//! tools (running inside `confluentinc/cp-kafka:6.1.1` containers) against
//! a Rust `crabka-broker` running on the host.
//!
//! Both tests are gated `#[ignore = "requires Docker"]` so `cargo test`
//! doesn't pull Docker by default. Run with `--ignored`.
//!
//! Networking: we use ad-hoc `docker run --rm --network host` per command
//! rather than a long-lived testcontainers Kafka. With host networking
//! the container shares the host's network namespace, so `127.0.0.1:9092`
//! inside the container points at our broker. This avoids the testcontainers
//! per-test bridge network problem where the host's Docker bridge gateway
//! IP isn't reachable.

#![cfg(not(target_os = "windows"))]

use std::io::Write;
use std::process::{Command, Stdio};

use crabka_broker::{Broker, BrokerConfig};
use crabka_log::LogConfig;

const HOST_PORT: u16 = 9092;
const BOOTSTRAP: &str = "127.0.0.1:9092";
const KAFKA_IMAGE: &str = "confluentinc/cp-kafka:6.1.1";

/// Spawn the broker, listening on `127.0.0.1:HOST_PORT`. With
/// `docker run --network host`, the container reaches us via the host's
/// loopback, so the advertised listener can simply be `BOOTSTRAP`.
async fn start_host_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr = BOOTSTRAP.parse().expect("static addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: BOOTSTRAP.into(),
        log_dir: dir.path().to_path_buf(),
        log_config: LogConfig::default(),
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!("CRABKA[test] broker started listen={BOOTSTRAP}");
    tracing::info!(listen = %BOOTSTRAP, "broker started for jvm acceptance");
    (handle, dir)
}

/// Verify TCP connectivity from inside a `--network host` container.
fn nc_check_connectivity() {
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "host",
            "alpine",
            "sh",
            "-c",
            "apk add --no-cache netcat-openbsd >/dev/null 2>&1 && nc -zv 127.0.0.1 9092",
        ])
        .output()
        .expect("spawn nc check");
    eprintln!(
        "NC CHECK status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Run `docker run --rm --network host <image> <args...>`, asserting success.
fn docker_run_kafka_tool(args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("--network")
        .arg("host")
        .arg("-e")
        .arg("KAFKA_TOOLS_LOG4J_LOGLEVEL=DEBUG")
        .arg(KAFKA_IMAGE)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn docker run");
    eprintln!(
        "CRABKA[test] docker_run {args:?} status={} stderr_len={} stderr_tail={}",
        out.status,
        out.stderr.len(),
        // print the tail (last 4KB) of stderr so we see Java's debug logs even on success
        String::from_utf8_lossy(if out.stderr.len() > 4096 {
            &out.stderr[out.stderr.len() - 4096..]
        } else {
            &out.stderr[..]
        }),
    );
    assert!(
        out.status.success(),
        "docker run {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn console_producer_round_trip() {
    const TOPIC: &str = "crabka-broker-itest";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    // 1. Create the topic via the JVM client.
    docker_run_kafka_tool(&[
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
        BOOTSTRAP,
    ]);

    // 2. Produce 3 records via stdin.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--network",
            "host",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"alpha\nbravo\ncharlie\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // 3. Consume them back via --partition 0 (bypasses groups entirely).
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "3",
        "--timeout-ms",
        "10000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(s.contains(needle), "consumer didn't emit {needle}: {s:?}");
    }

    broker.shutdown().await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_topics_describe_smokes_metadata() {
    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        "described",
        "--partitions",
        "2",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-topics",
        "--describe",
        "--topic",
        "described",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
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
    let _ = HOST_PORT; // silence dead_code on Windows builds
}
