//! JVM-acceptance tests for KIP-848 — drives the GA Kafka 4.0 client
//! against an in-process Crabka broker. `group.protocol=consumer`
//! activates the next-gen heartbeat path on the client.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use assert2::assert;
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

/// Live classic ↔ next-gen migration within a *single* consumer group.
///
/// A classic (cp-kafka 7.4.0) member forms the group first; while it is still
/// polling, a next-gen (apache/kafka 4.0.0, `group.protocol=consumer`) member
/// joins the SAME group. Crabka's unified coordinator runs the default
/// `Bidirectional` migration policy with the `consumer` rebalance protocol
/// enabled (see `NextGenConfig::default`; `start_host_broker` does not override
/// it), so the group upgrades in place. The assertion is that both members
/// consume a non-empty, *disjoint* set of partitions whose union covers the
/// whole topic — i.e. a coherent assignment spanning both protocols rather
/// than a split-brain double-assignment.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires Docker"]
async fn jvm_kip848_classic_and_consumer_in_one_group_migrate() {
    let (broker, _dir) = start_host_broker().await;
    create_topic("mig", 4);

    // Produce 8 records, 2 per partition, deterministically. Kafka's default
    // partitioner is murmur2(key) % numPartitions, so keyed records land on a
    // fixed partition regardless of batching (unlike RoundRobinPartitioner,
    // which advances per-batch and can leave partitions empty). For 4
    // partitions the keys below map: "0"->0, "4"->1, "5"->2, "1"->3.
    let produced = docker_run(
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
    assert!(produced.status.success(), "producer failed: {produced:?}");

    let group = "g-migrate";

    // Classic member (cp-kafka): joins first and stays in the group long enough
    // to overlap the next-gen member. Prints the partition for each record.
    let classic = spawn_consumer(
        KAFKA_IMAGE_CLASSIC,
        format!(
            "kafka-console-consumer --bootstrap-server {BOOTSTRAP} --topic mig --group {group} \
             --from-beginning --property print.partition=true --timeout-ms 30000 --max-messages 8" // 8 = total records produced; using the total rather than per-member
                                                                                                   // expectation (4) avoids a false-negative when the assignor produces a
                                                                                                   // 3/1 split: the member owning 3 partitions needs 6 records but would
                                                                                                   // stop at 4. With --max-messages 8 each consumer drains everything
                                                                                                   // assigned to it and exits on --timeout-ms once records run dry.
        ),
    );

    // Let the classic member form/own the group before the next-gen joins.
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    // Next-gen member (apache/kafka, group.protocol=consumer): joins the SAME
    // group, triggering an in-place upgrade under the Bidirectional policy.
    let nextgen = spawn_consumer(
        KAFKA_IMAGE_NEXT_GEN,
        format!(
            "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic mig \
             --group {group} --consumer-property group.protocol=consumer --from-beginning \
             --property print.partition=true --timeout-ms 30000 --max-messages 8" // 8 = total records; see comment on classic consumer above.
        ),
    );

    let classic_out = classic.await.unwrap();
    let nextgen_out = nextgen.await.unwrap();
    eprintln!("CRABKA[test] classic stdout:\n{classic_out}");
    eprintln!("CRABKA[test] nextgen stdout:\n{nextgen_out}");

    let cp = parse_partitions(&classic_out);
    let np = parse_partitions(&nextgen_out);
    assert!(
        !cp.is_empty() && !np.is_empty(),
        "both members consumed: classic={cp:?} nextgen={np:?}"
    );
    assert!(
        cp.is_disjoint(&np),
        "no partition overlap across protocols: classic={cp:?} nextgen={np:?}"
    );
    let union: std::collections::BTreeSet<i32> = cp.union(&np).copied().collect();
    let all: std::collections::BTreeSet<i32> = (0..4).collect();
    assert!(union == all, "union covers all partitions: {union:?}");

    // The migrating group must describe coherently to the JVM admin tooling.
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
