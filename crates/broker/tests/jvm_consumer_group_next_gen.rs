//! JVM-acceptance tests for KIP-848 — drives the GA Kafka 4.0 client
//! against an in-process Crabka broker. `group.protocol=consumer`
//! activates the next-gen heartbeat path on the client.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::process::{Command, Stdio};

use crabka_broker::{Broker, BrokerConfig};
use crabka_log::LogConfig;

const BOOTSTRAP: &str = "host.docker.internal:9092";
const LISTEN: &str = "0.0.0.0:9092";
const KAFKA_IMAGE_NEXT_GEN: &str = "apache/kafka:4.0.0";
const KAFKA_IMAGE_CLASSIC: &str = "confluentinc/cp-kafka:7.4.0";

async fn start_host_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = LISTEN.parse().expect("static addr");
    let controller_addr: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: BOOTSTRAP.into(),
        log_dir: dir.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: 1,
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(1, controller_addr)],
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: std::time::Duration::from_secs(5),
        controller_heartbeat_interval: std::time::Duration::from_millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!("CRABKA[test] broker started listen={LISTEN} advertised={BOOTSTRAP}");
    (handle, dir)
}

/// Pre-create a topic via the classic admin tooling. Crabka's broker does
/// not auto-create topics on the produce path; tests must establish them
/// explicitly, matching the existing `jvm_acceptance.rs` convention.
fn create_topic(name: &str, partitions: i32) {
    let out = docker_run(
        KAFKA_IMAGE_CLASSIC,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            name,
            "--partitions",
            &partitions.to_string(),
            "--replication-factor",
            "1",
        ],
    );
    assert!(
        out.status.success(),
        "create topic {name} failed: stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Run a docker container and return its output without asserting success.
/// Consumer commands often exit non-zero on timeout even when they consumed
/// messages, so callers are responsible for checking what matters.
fn docker_run(image: &str, args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(image)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("docker run");
    eprintln!(
        "CRABKA[test] docker {image} {args:?} status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_single_consumer_round_trip() {
    let (_broker, _dir) = start_host_broker().await;
    create_topic("kip848-rt", 1);
    let produced = docker_run(
        KAFKA_IMAGE_CLASSIC,
        &[
            "bash",
            "-c",
            &format!(
                "printf 'a\\nb\\nc\\n' | kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic kip848-rt --producer-property max.block.ms=10000"
            ),
        ],
    );
    assert!(produced.status.success(), "producer failed: {produced:?}");

    let consumed = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &[
            "bash",
            "-c",
            &format!(
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-rt --group g-rt --consumer-property group.protocol=consumer --from-beginning --timeout-ms 8000 --max-messages 3"
            ),
        ],
    );
    let stdout = String::from_utf8_lossy(&consumed.stdout);
    assert!(
        stdout.contains('a') && stdout.contains('b') && stdout.contains('c'),
        "expected a/b/c, got {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_describe_group() {
    let (_broker, _dir) = start_host_broker().await;
    create_topic("kip848-d", 1);
    docker_run(
        KAFKA_IMAGE_CLASSIC,
        &[
            "bash",
            "-c",
            &format!(
                "printf '1\\n2\\n' | kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic kip848-d"
            ),
        ],
    );
    let _ = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &[
            "bash",
            "-c",
            &format!(
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-d --group g-d --consumer-property group.protocol=consumer --from-beginning --timeout-ms 6000 --max-messages 2"
            ),
        ],
    );
    let described = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &[
            "bash",
            "-c",
            &format!(
                "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {BOOTSTRAP} --describe --group g-d"
            ),
        ],
    );
    let stdout = String::from_utf8_lossy(&described.stdout);
    assert!(
        stdout.contains("g-d"),
        "expected group g-d in describe output, got {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_delete_group() {
    let (_broker, _dir) = start_host_broker().await;
    create_topic("kip848-del", 1);
    docker_run(
        KAFKA_IMAGE_CLASSIC,
        &[
            "bash",
            "-c",
            &format!(
                "printf 'x\\n' | kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic kip848-del"
            ),
        ],
    );
    let _ = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &[
            "bash",
            "-c",
            &format!(
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-del --group g-del --consumer-property group.protocol=consumer --from-beginning --timeout-ms 4000 --max-messages 1"
            ),
        ],
    );
    let deleted = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &[
            "bash",
            "-c",
            &format!(
                "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {BOOTSTRAP} --delete --group g-del"
            ),
        ],
    );
    assert!(deleted.status.success(), "delete failed: {deleted:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_coexists_with_classic() {
    let (_broker, _dir) = start_host_broker().await;
    create_topic("kip848-coex", 1);
    docker_run(
        KAFKA_IMAGE_CLASSIC,
        &[
            "bash",
            "-c",
            &format!(
                "printf 'p\\nq\\n' | kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic kip848-coex"
            ),
        ],
    );
    let classic = docker_run(
        KAFKA_IMAGE_CLASSIC,
        &[
            "bash",
            "-c",
            &format!(
                "kafka-console-consumer --bootstrap-server {BOOTSTRAP} --topic kip848-coex --group g-classic --from-beginning --timeout-ms 5000 --max-messages 2"
            ),
        ],
    );
    let cs = String::from_utf8_lossy(&classic.stdout);
    assert!(cs.contains('p') && cs.contains('q'));

    let next_gen = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &[
            "bash",
            "-c",
            &format!(
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-coex --group g-next --consumer-property group.protocol=consumer --from-beginning --timeout-ms 5000 --max-messages 2"
            ),
        ],
    );
    let ns = String::from_utf8_lossy(&next_gen.stdout);
    assert!(ns.contains('p') && ns.contains('q'));
}
