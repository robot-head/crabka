//! JVM-acceptance tests for KIP-848 — drives the GA Kafka 4.0 client
//! against an in-process Crabka broker. `group.protocol=consumer`
//! activates the next-gen heartbeat path on the client.

#![allow(clippy::pedantic)]

use std::process::{Command, Stdio};

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig};
use crabka_log::LogConfig;

const BOOTSTRAP: &str = "host.docker.internal:9092";
const LISTEN: &str = "0.0.0.0:9092";
const KAFKA_IMAGE_NEXT_GEN: &str = "mirror.gcr.io/apache/kafka:4.0.0";
const KAFKA_IMAGE_CLASSIC: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.4.0";

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
        controller_quorum_voters: vec![(1, controller_addr.to_string())],
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

/// Spawn a console-consumer container on a blocking thread and return a handle
/// resolving to its stdout. Used to run two overlapping consumers in the same
/// group: the caller awaits both handles, so the containers run concurrently
/// rather than back-to-back. `--add-host` mirrors `docker_run` so the
/// container can reach the host-process broker via `host.docker.internal`.
fn spawn_consumer(image: &'static str, script: String) -> tokio::task::JoinHandle<String> {
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new("docker")
            .arg("run")
            .arg("--rm")
            .arg("--add-host=host.docker.internal:host-gateway")
            .arg(image)
            .arg("bash")
            .arg("-c")
            .arg(&script)
            .output()
            .expect("docker run");
        eprintln!(
            "CRABKA[test] consumer {image} status={} stderr={}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    })
}

/// Extract the set of partition numbers from console-consumer stdout produced
/// with `--property print.partition=true`. The `DefaultMessageFormatter`
/// emits one `Partition:<n>` token per record line (e.g. `Partition:2\t<value>`
/// when no key is printed); we tolerate any surrounding columns and just pull
/// the integer following each `Partition:` marker.
fn parse_partitions(stdout: &str) -> std::collections::BTreeSet<i32> {
    let mut set = std::collections::BTreeSet::new();
    for line in stdout.lines() {
        for token in line.split(['\t', ' ']) {
            if let Some(rest) = token.strip_prefix("Partition:")
                && let Ok(n) = rest.trim().parse::<i32>()
            {
                set.insert(n);
            }
        }
    }
    set
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
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-rt --group g-rt --consumer-property group.protocol=consumer --from-beginning --timeout-ms 10000 --max-messages 3"
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
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-d --group g-d --consumer-property group.protocol=consumer --from-beginning --timeout-ms 10000 --max-messages 2"
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
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-del --group g-del --consumer-property group.protocol=consumer --from-beginning --timeout-ms 10000 --max-messages 1"
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
                "kafka-console-consumer --bootstrap-server {BOOTSTRAP} --topic kip848-coex --group g-classic --from-beginning --timeout-ms 10000 --max-messages 2"
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
                "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-coex --group g-next --consumer-property group.protocol=consumer --from-beginning --timeout-ms 10000 --max-messages 2"
            ),
        ],
    );
    let ns = String::from_utf8_lossy(&next_gen.stdout);
    assert!(ns.contains('p') && ns.contains('q'));
}

/// Migration interop within a *single* consumer group, deterministically.
///
/// Phase 1: a classic (cp-kafka 7.4.0) consumer forms group `g-migrate` and
/// drains batch 1 — proving the group is served by the classic protocol. Phase
/// 2: a next-gen (apache/kafka 4.0.0, `group.protocol=consumer`) consumer joins
/// the SAME group and drains a freshly-produced batch 2. Crabka's unified
/// coordinator runs the default `Bidirectional` policy with the consumer
/// rebalance protocol enabled (`NextGenConfig::default`; `start_host_broker`
/// does not override it), so the group is served to the consumer protocol in
/// place, and the next-gen member reads batch 2 from the offsets the classic
/// member committed — i.e. both protocols work against the same group with
/// offset continuity across the migration.
///
/// Each phase runs a SOLE member that owns all partitions, so the assignment is
/// fixed and there is no concurrency/rebalance race (an earlier concurrent
/// design flaked on CI: the lone classic member drained every record and
/// committed offsets before the next-gen joined, starving it). The live,
/// concurrent mixed-membership split is covered deterministically by the
/// in-process suite in `coordinator::unified` (upgrade / downgrade / round-trip
/// / gap-free assignment / static-membership / committed-offset survival).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_classic_and_consumer_in_one_group_migrate() {
    let (broker, _dir) = start_host_broker().await;
    create_topic("mig", 4);
    let group = "g-migrate";
    let all: std::collections::BTreeSet<i32> = (0..4).collect();

    // Produce one deterministic batch of 8 records (2 per partition). Kafka's
    // default partitioner is murmur2(key) % numPartitions, so keyed records land
    // on a fixed partition: "0"->0, "4"->1, "5"->2, "1"->3.
    let produce = |label: &str| {
        let out = docker_run(
            KAFKA_IMAGE_CLASSIC,
            &[
                "bash",
                "-c",
                &format!(
                    "printf '0:a\\n4:b\\n5:c\\n1:d\\n0:e\\n4:f\\n5:g\\n1:h\\n' | \
                     kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic mig \
                     --property parse.key=true --property key.separator=: \
                     --producer-property max.block.ms=15000"
                ),
            ],
        );
        assert!(out.status.success(), "{label} producer failed: {out:?}");
    };

    // Phase 1 — classic consumer drains batch 1 from all four partitions.
    produce("batch1");
    let classic_out = spawn_consumer(
        KAFKA_IMAGE_CLASSIC,
        format!(
            "kafka-console-consumer --bootstrap-server {BOOTSTRAP} --topic mig --group {group} \
             --from-beginning --property print.partition=true --timeout-ms 25000 --max-messages 8"
        ),
    )
    .await
    .unwrap();
    eprintln!("CRABKA[test] classic stdout:\n{classic_out}");
    let cp = parse_partitions(&classic_out);
    assert!(
        cp == all,
        "classic consumer must cover all partitions: {cp:?}\nstdout: {classic_out}"
    );

    // Phase 2 — a next-gen consumer joins the SAME group (in-place migration to
    // the consumer protocol) and drains batch 2 from the classic-committed
    // offsets, across all four partitions.
    produce("batch2");
    let nextgen_out = spawn_consumer(
        KAFKA_IMAGE_NEXT_GEN,
        format!(
            "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic mig \
             --group {group} --consumer-property group.protocol=consumer --from-beginning \
             --property print.partition=true --timeout-ms 25000 --max-messages 8"
        ),
    )
    .await
    .unwrap();
    eprintln!("CRABKA[test] nextgen stdout:\n{nextgen_out}");
    let np = parse_partitions(&nextgen_out);
    assert!(
        np == all,
        "next-gen consumer must cover all partitions after the migration: {np:?}\nstdout: {nextgen_out}"
    );

    // The migrated group must describe coherently to the JVM admin tooling.
    let describe = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &[
            "bash",
            "-c",
            &format!(
                "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {BOOTSTRAP} --describe --group {group}"
            ),
        ],
    );
    assert!(
        String::from_utf8_lossy(&describe.stdout).contains("mig"),
        "describe mentions topic mig: {}",
        String::from_utf8_lossy(&describe.stdout),
    );

    drop(broker);
}
