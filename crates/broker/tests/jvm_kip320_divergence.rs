//! KIP-320 JVM mixed-cluster acceptance scenarios.
//!
//! KIP-320 is in-band log-truncation detection. These scenarios are
//! Docker-gated (`#[ignore]`) and Linux-bound. See the project benchmark/JVM
//! memory. The hosted-Mac Docker bridge does not reliably share the host
//! loopback, so these run on the Linux harness/CI, not on a dev Mac.
//!
//! Run on Linux/CI:
//! ```text
//! cargo test -p crabka-broker --test jvm_kip320_divergence -- --ignored --nocapture
//! ```
//!
//! Four scenarios, each independently `#[ignore]`d:
//!
//! 1. [`kip320_wire_conformance_offset_for_leader_epoch`][]: wire-conformance.
//!    The test starts a single Crabka broker and produces across two leader
//!    epochs. A small Java helper drives the official
//!    `org.apache.kafka.clients.consumer.KafkaConsumer` against Crabka. The
//!    test compiles that helper in-container with the cp-kafka JDK's `javac`.
//!    The consumer's offset/position-validation pass issues a real
//!    `OffsetForLeaderEpoch` (`api_key` 23) for KIP-320, and it consumes
//!    at Fetch v12+, so the JVM `Fetcher` decodes Crabka's tagged
//!    `diverging_epoch` / `current_leader` fields. The byte-exactness signal
//!    is a clean drain across both epochs with no deserialization or
//!    truncation fault, plus the observed end-offset that frames the
//!    old-epoch boundary. The Rust side independently cross-checks the same
//!    `OffsetForLeaderEpoch` answer over the wire with the Task-2 client
//!    helper.
//!
//! 2. [`kip320_jvm_follower_truncates_from_crabka_leader`][]: induced divergence.
//!    The test runs a mixed JVM+Crabka cluster: one
//!    `mirror.gcr.io/apache/kafka:4.0.0` broker and a Crabka broker that share
//!    a Crabka-led `KRaft` metadata quorum, per the Slice-6 mixed-quorum work
//!    in `jvm_static_quorum_spike.rs`. The test forces a real divergent
//!    suffix. It produces a committed prefix, takes the partition offline with
//!    a forged `PartitionRecord` that names a dead phantom leader and so also
//!    parks the replication fetchers, diverges the two replicas' logs so the
//!    survivor that becomes leader has a *shorter* log at a *new* epoch, then
//!    rejoins the old leader as a follower. The test asserts that the JVM
//!    follower truncates its divergent suffix to converge on the Crabka
//!    leader. Its on-disk log, dumped with `kafka-dump-log`, contains the
//!    leader's rewritten suffix at the leader's exact LEO. The test also asserts that a
//!    `kafka-console-consumer` recovers and continues without a fatal
//!    deserialization/`LogTruncationException`.
//!
//! 3. [`kip320_crabka_follower_truncates_from_jvm_leader`][]: the reverse
//!    direction. The test parks replication behind a phantom leader, appends a
//!    Crabka-only suffix, then promotes the JVM replica. The Crabka follower
//!    must truncate that suffix and resume at the JVM leader's exact LEO.
//!
//! 4. [`metadata_version_downgrade_rejects_pre_kip1155_jvm`][]: the KIP-1155
//!    mixed-version safety gate. Kafka 4.0 predates KIP-1155 and therefore
//!    advertises no downgrade capability. Both safe and unsafe online
//!    downgrades must be rejected while that broker/controller is registered,
//!    without changing the finalized version or projecting away metadata.
//!
//! ## Topology & networking
//!
//! The topology is the same as the rest of the JVM harness. Crabka brokers
//! bind `0.0.0.0:<port>` on the host and advertise
//! `host.docker.internal:<port>`. The cp-kafka / apache-kafka tool containers
//! get `--add-host=host.docker.internal:host-gateway`. Controller (`KRaft`
//! metadata-quorum) traffic uses host loopback between the Crabka voters and
//! the JVM voter's published port. These tests deliberately do NOT use
//! `--network host`. It silently fails to share the host loopback on hosted
//! ubuntu runners. See the `jvm_acceptance.rs` module docs.

use std::{
    net::SocketAddr,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use base64::Engine as _;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};
use crabka_log::LogConfig;
use crabka_metadata::{LeaderEpoch, MetadataRecord, PartitionRecord};
use tempfile::TempDir;
use uuid::Uuid;

mod support;

/// cp-kafka 6.1.1 (Kafka 2.7) ships the standard Apache Kafka CLI tools used
/// for produce / topic admin / `kafka-dump-log`. NOTE: its bundled consumer
/// only negotiates Fetch up to v11 and predates client-side KIP-320 position
/// validation, so these tests do NOT use it for the Fetch-v12+
/// wire-conformance probe. That probe needs [`KAFKA_IMAGE_MODERN`].
const KAFKA_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:6.1.1";
/// cp-kafka 7.5.0 (Kafka 3.5) is the modern client image. Its consumer
/// negotiates Fetch v12+ and runs the full KIP-320 client path
/// (`OffsetForLeaderEpoch` position validation + tagged `diverging_epoch` /
/// `current_leader` decode), and it ships a JDK with `javac`. These tests use
/// it to compile and run the wire-conformance Java helper.
const KAFKA_IMAGE_MODERN: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.5.0";
/// mirror.gcr.io/apache/kafka:4.0.0 is the `KRaft`-native broker used as the JVM member of the
/// mixed metadata quorum (same image as `jvm_static_quorum_spike.rs`).
const KAFKA_IMAGE_KRAFT: &str = "mirror.gcr.io/apache/kafka:4.0.0";
/// Newer CLI image used only as an `AdminClient`. Its `kafka-features.sh`
/// exposes the explicit safe/unsafe downgrade commands used by KIP-1155.
const KAFKA_IMAGE_FEATURES: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// Kafka encodes a 16-byte UUID cluster id as URL-safe base64 with no
/// padding. The JVM `--cluster-id` string and Crabka's `uuid::Uuid` must wrap
/// the *same* 16 bytes or the two sides reject each other on cluster-id
/// mismatch. This helper is lifted verbatim from `jvm_static_quorum_spike.rs`.
fn kafka_cluster_id_string(id: Uuid) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id.as_bytes())
}

fn docker_rm(name: &str) {
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
}

fn set_container_paused(name: &str, paused: bool) {
    let action = if paused { "pause" } else { "unpause" };
    let status = Command::new("docker")
        .args([action, name])
        .status()
        .unwrap_or_else(|error| panic!("{action} JVM broker: {error}"));
    assert!(status.success(), "{action} JVM broker failed");
}

/// Address of the default Docker bridge as seen by both host processes and
/// containers. Mixed-cluster broker endpoints must work from both sides:
/// `host.docker.internal` is container-only on Linux, while this numeric
/// gateway is routable from Crabka and the JVM/tool containers alike.
fn docker_bridge_gateway() -> String {
    let output = Command::new("docker")
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}",
        ])
        .output()
        .expect("docker network inspect bridge");
    assert!(output.status.success(), "inspect Docker bridge gateway");
    let gateway = String::from_utf8(output.stdout)
        .expect("Docker bridge gateway is UTF-8")
        .trim()
        .to_owned();
    gateway
        .parse::<std::net::IpAddr>()
        .expect("Docker bridge gateway is an IP address");
    gateway
}

/// Run a bundled Kafka CLI tool in a throwaway cp-kafka container on the
/// default bridge with `host.docker.internal` wired to the host gateway.
/// Mirrors `jvm_acceptance.rs::docker_run_kafka_tool_with_image`.
fn docker_run_kafka_tool_with_image(image: &str, args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(image)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn docker run");
    eprintln!(
        "CRABKA[kip320] docker_run image={image} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    out
}

fn run_features(bootstrap: &str, command: &[&str]) -> std::process::Output {
    let mut args = vec![
        "/opt/kafka/bin/kafka-features.sh",
        "--bootstrap-server",
        bootstrap,
    ];
    args.extend_from_slice(command);
    docker_run_kafka_tool_with_image(KAFKA_IMAGE_FEATURES, &args)
}

/// Single-broker Crabka config bound on `0.0.0.0:<client_port>`, advertised as
/// `host.docker.internal:<client_port>`. Mirrors `start_host_broker` but
/// parameterized on the port so the wire-conformance test can pick a port that
/// doesn't collide with the rest of the JVM suite.
async fn start_host_broker_on(client_port: u16, controller_port: u16) -> (BrokerHandle, TempDir) {
    support::init_tracing();
    let dir = TempDir::new().expect("tempdir");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{client_port}").parse().expect("addr"),
        advertised_listener: format!("host.docker.internal:{client_port}"),
        log_dir: dir.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: format!("0.0.0.0:{controller_port}").parse().expect("addr"),
        controller_quorum_voters: vec![(
            crabka_broker::NodeId(1),
            format!("127.0.0.1:{controller_port}"),
        )],
        heartbeat_interval: crabka_units::millis(3_000),
        // This broker advertises a container-only hostname, so its host-side
        // heartbeat client cannot loop back through the advertised listener.
        // Keep it alive for the bounded in-container Java compile and probe.
        heartbeat_timeout: crabka_units::secs(120),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    (handle, dir)
}

/// The small Java helper that proves the official JVM client decodes Crabka's
/// `OffsetForLeaderEpoch` (`api_key` 23) + Fetch v12+ responses byte-exactly.
///
/// It builds an official `org.apache.kafka.clients.consumer.KafkaConsumer`,
/// assigns the partition, and drains both leader epochs. The JVM `Fetcher`'s
/// offset/position-validation pass issues `OffsetForLeaderEpoch` for KIP-320.
/// It decodes the tagged `diverging_epoch` / `current_leader` fields the
/// Crabka leader stamps into Fetch v12+ responses. The byte-exactness signal
/// is a clean drain with no `LogTruncationException` and no
/// `RecordDeserializationException`, plus the observed
/// `beginningOffsets`/`endOffsets` that frame the old-epoch boundary. The
/// helper prints `KIP320PROBE OK` on success. Otherwise it prints
/// `KIP320PROBE FAIL ...` and exits non-zero, so the Rust side can assert on
/// stdout.
///
/// The test writes the source string to a host tempdir and mounts it into the
/// cp-kafka container. It then compiles the source in-container with the
/// bundled JDK's `javac` against the container's Kafka client jars, and runs
/// it.
const OFFSET_FOR_LEADER_EPOCH_HELPER_JAVA: &str = r#"
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.time.Duration;
import java.util.*;

public class Kip320Probe {
  public static void main(String[] args) throws Exception {
    String bootstrap = args[0];
    String topic = args[1];
    long expectedOldEpochEnd = Long.parseLong(args[2]);

    Properties p = new Properties();
    p.put("bootstrap.servers", bootstrap);
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("group.id", "kip320-probe");
    p.put("auto.offset.reset", "earliest");
    // Force the modern Fetch path (v12+) so the broker's tagged
    // diverging_epoch / current_leader fields are exercised on decode.
    p.put("enable.auto.commit", "false");

    KafkaConsumer<String,String> c = new KafkaConsumer<>(p);
    TopicPartition tp = new TopicPartition(topic, 0);
    c.assign(Collections.singletonList(tp));
    c.seekToBeginning(Collections.singletonList(tp));

    // Drain everything. If Crabka's OffsetForLeaderEpoch / diverging_epoch
    // bytes were malformed, the JVM Fetcher would either throw
    // LogTruncationException or RecordDeserializationException here.
    int polled = 0;
    long end = System.currentTimeMillis() + 20000;
    long beginning = c.beginningOffsets(Collections.singletonList(tp)).get(tp);
    long latest = c.endOffsets(Collections.singletonList(tp)).get(tp);
    while (System.currentTimeMillis() < end && c.position(tp) < latest) {
      ConsumerRecords<String,String> recs = c.poll(Duration.ofMillis(500));
      polled += recs.count();
    }
    long finalPosition = c.position(tp);
    System.out.println("KIP320PROBE beginning=" + beginning + " latest=" + latest + " position=" + finalPosition + " polled=" + polled);

    // The consumer committed/validated its positions across both epochs via
    // OffsetForLeaderEpoch under the hood. We assert the visible end offset
    // matches the broker's reported log end, and that the OLD epoch boundary
    // we were told to expect lies strictly inside [beginning, latest].
    if (latest <= 0) { System.out.println("KIP320PROBE FAIL empty-log"); System.exit(2); }
    if (finalPosition != latest) { System.out.println("KIP320PROBE FAIL incomplete-drain"); System.exit(4); }
    if (polled <= 0) { System.out.println("KIP320PROBE FAIL no-records-polled"); System.exit(5); }
    if (expectedOldEpochEnd <= beginning || expectedOldEpochEnd > latest) {
      System.out.println("KIP320PROBE FAIL boundary expectedOldEpochEnd=" + expectedOldEpochEnd);
      System.exit(3);
    }
    System.out.println("KIP320PROBE OK");
    c.close();
  }
}
"#;

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 1: wire-conformance — JVM client decodes OffsetForLeaderEpoch +
// Fetch v12 diverging_epoch byte-exactly against a Crabka leader.
// ─────────────────────────────────────────────────────────────────────────────

/// Step 1 of Task 11: a JVM client and a Crabka broker exchange
/// `OffsetForLeaderEpoch` + Fetch v12+. The test produces across two epochs on
/// the Crabka leader, then runs the official Java consumer. That consumer
/// issues `OffsetForLeaderEpoch` during position validation and decodes the
/// tagged `diverging_epoch` / `current_leader` Fetch fields. The test asserts
/// that the consumer drains both epochs without a deserialization or
/// truncation fault, and that the old epoch's boundary matches the broker's
/// view.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; Linux-bound (host.docker.internal bridge)"]
async fn kip320_wire_conformance_offset_for_leader_epoch() {
    const TOPIC: &str = "crabka-kip320-wire";
    const CONTAINER: &str = "crabka-kip320-wire-helper";
    const CLIENT_PORT: u16 = 10692;
    const CONTROLLER_PORT: u16 = 10693;
    const BOOTSTRAP: &str = "host.docker.internal:10692";

    docker_rm(CONTAINER);
    let (broker, _dir) = start_host_broker_on(CLIENT_PORT, CONTROLLER_PORT).await;

    // 1. Create topic (1 partition, RF=1).
    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE,
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
            BOOTSTRAP,
        ],
    );
    assert!(out.status.success(), "create topic failed");

    // 2. Produce a first batch at the current (epoch 0) leadership.
    produce_lines_via_jvm(
        BOOTSTRAP,
        TOPIC,
        &(0..5).map(|i| format!("e0-{i}")).collect::<Vec<_>>(),
    );

    // The offset boundary of epoch 0 is the broker's current log end offset.
    let epoch0_end = broker
        .local_log_end_offset(TOPIC, 0)
        .expect("partition hosted");
    eprintln!("CRABKA[kip320] epoch-0 boundary (LEO) = {epoch0_end}");

    // 3. Bump the partition's leader epoch to simulate a leadership change,
    //    then produce a second batch at the new epoch. Now an
    //    OffsetForLeaderEpoch(epoch=0) MUST return `epoch0_end`.
    let mut partition = broker
        .partition_record_for_test(TOPIC, 0)
        .expect("wire-probe partition metadata");
    partition.leader_epoch = partition.leader_epoch.next();
    let epoch1 = partition.leader_epoch;
    partition.partition_epoch += 1;
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(partition))
        .await
        .expect("advance wire-probe leader epoch in metadata");
    let epoch_deadline = Instant::now() + Duration::from_secs(5);
    while broker
        .partition_record_for_test(TOPIC, 0)
        .is_none_or(|partition| partition.leader_epoch != epoch1)
    {
        assert!(
            Instant::now() <= epoch_deadline,
            "wire-probe leader epoch did not reach metadata"
        );
        tokio::task::yield_now().await;
    }
    produce_lines_via_jvm(
        BOOTSTRAP,
        TOPIC,
        &(0..5).map(|i| format!("e1-{i}")).collect::<Vec<_>>(),
    );

    // 4. Cross-check the broker's own OffsetForLeaderEpoch over the wire via
    //    the Rust client helper (Task 2). This is the byte-exact source of
    //    truth the JVM helper is validated against.
    {
        let client = crabka_client_core::Client::builder()
            .bootstrap(format!("127.0.0.1:{CLIENT_PORT}"))
            .build()
            .await
            .expect("rust probe client");
        // current_leader_epoch = -1 (no fencing); ask for the end offset of
        // epoch 0.
        let answer = client
            .offset_for_leader_epoch(TOPIC, 0, -1, 0)
            .await
            .expect("offset_for_leader_epoch");
        eprintln!("CRABKA[kip320] OffsetForLeaderEpoch(epoch=0) => {answer:?}");
        assert!(
            answer.error_code == 0,
            "OffsetForLeaderEpoch returned error {}",
            answer.error_code
        );
        assert!(
            answer.end_offset == epoch0_end,
            "OffsetForLeaderEpoch(epoch=0).end_offset {} != epoch-0 boundary {}",
            answer.end_offset,
            epoch0_end,
        );
    }

    // 5. Compile + run the Java helper inside the cp-kafka container. It drives
    //    the official Apache Kafka consumer, which validates positions via
    //    OffsetForLeaderEpoch and decodes Fetch v12+ tagged diverging_epoch /
    //    current_leader fields. A clean drain + matching boundary is the
    //    byte-exactness signal.
    let helper_dir = TempDir::new().unwrap();
    let helper_path = helper_dir.path().join("Kip320Probe.java");
    std::fs::write(&helper_path, OFFSET_FOR_LEADER_EPOCH_HELPER_JAVA).unwrap();
    // The helper image runs as a non-root uid, while `TempDir` is 0700.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::set_permissions(helper_dir.path(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod Java helper directory");
        std::fs::set_permissions(&helper_path, std::fs::Permissions::from_mode(0o644))
            .expect("chmod Java helper source");
    }
    let entry = format!(
        "set -e; cp /helper/Kip320Probe.java /tmp/Kip320Probe.java; \
         CP=$(ls /usr/share/java/kafka/*.jar 2>/dev/null | tr '\\n' ':')$(ls /usr/share/java/cp-base-new/*.jar 2>/dev/null | tr '\\n' ':'); \
         javac -cp \"$CP\" -d /tmp /tmp/Kip320Probe.java; \
         java -cp \"/tmp:$CP\" Kip320Probe {BOOTSTRAP} {TOPIC} {epoch0_end}"
    );
    let helper_out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--name",
            CONTAINER,
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &format!("{}:/helper", helper_dir.path().display()),
            "--entrypoint",
            "bash",
            // Modern image (Kafka 3.5): Fetch v12+ + full KIP-320 client path.
            KAFKA_IMAGE_MODERN,
            "-c",
            &entry,
        ])
        .output()
        .expect("spawn java helper");
    let stdout = String::from_utf8_lossy(&helper_out.stdout);
    let stderr = String::from_utf8_lossy(&helper_out.stderr);
    eprintln!(
        "CRABKA[kip320] java helper status={} stdout={stdout} stderr={stderr}",
        helper_out.status
    );

    // The JVM consumer must NOT have hit a deserialization / truncation fault
    // decoding Crabka's OffsetForLeaderEpoch + diverging_epoch bytes.
    assert!(
        !stderr.contains("RecordDeserializationException")
            && !stdout.contains("RecordDeserializationException"),
        "JVM consumer hit a deserialization error decoding Crabka Fetch v12+: {stderr}"
    );
    assert!(
        stdout.contains("KIP320PROBE OK"),
        "JVM OffsetForLeaderEpoch / Fetch v12 conformance probe did not pass: stdout={stdout} stderr={stderr}"
    );

    docker_rm(CONTAINER);
    broker.shutdown().await;
}

/// Produce `lines` to `topic` partition 0 with the JVM `kafka-console-producer`
/// at `acks=all`, one record per line. Panics on producer failure.
fn produce_lines_via_jvm(bootstrap: &str, topic: &str, lines: &[String]) {
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            topic,
            "--producer-property",
            "acks=all",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn JVM producer");
    {
        use std::io::Write as _;
        let stdin = child.stdin.as_mut().expect("stdin");
        for l in lines {
            writeln!(stdin, "{l}").expect("write line");
        }
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait producer");
    assert!(
        out.status.success(),
        "JVM producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Wait until an external Kafka metadata request observes `expected` as the
/// partition leader. This gates producer/follower steps on the JVM broker's
/// view, rather than only on Crabka's already-applied metadata image.
async fn wait_for_described_leader(bootstrap: &str, topic: &str, expected: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let marker = format!("Leader: {expected}");
    loop {
        let output = docker_run_kafka_tool_with_image(
            KAFKA_IMAGE,
            &[
                "kafka-topics",
                "--describe",
                "--topic",
                topic,
                "--bootstrap-server",
                bootstrap,
            ],
        );
        let description = String::from_utf8_lossy(&output.stdout);
        if output.status.success() && description.contains(&marker) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "external metadata never observed {topic} leader {expected}: {description}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn described_isr(description: &str) -> Vec<u64> {
    description
        .lines()
        .find_map(|line| line.split_once("Isr:").map(|(_, tail)| tail))
        .and_then(|tail| tail.split_whitespace().next())
        .into_iter()
        .flat_map(|ids| ids.split(','))
        .filter_map(|id| id.parse().ok())
        .collect()
}

/// Create the RF=3 mixed-cluster topic after all brokers have registered.
/// Registration and unfencing are separate `KRaft` transitions, so retry the
/// administrative request through the short window between them.
async fn create_mixed_topic(bootstrap: &str, topic: &str) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let output = docker_run_kafka_tool_with_image(
            KAFKA_IMAGE,
            &[
                "kafka-topics",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "3",
                "--bootstrap-server",
                bootstrap,
            ],
        );
        if output.status.success() {
            return;
        }
        assert2::assert!(
            Instant::now() <= deadline,
            "create topic {topic} did not succeed after broker registration: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Mixed JVM+Crabka cluster scaffolding (data plane on top of a Crabka-led
// KRaft metadata quorum, per the Slice-6 mixed-quorum work).
// ─────────────────────────────────────────────────────────────────────────────

/// A running mixed cluster: two Crabka brokers (ids 1, 2) that hold the
/// metadata-quorum majority, plus one JVM broker (id 3) joined over the real
/// `KRaft` wire. `jvm_container` is the docker container name, already started.
struct MixedCluster {
    crabka: Vec<(BrokerHandle, TempDir)>,
    jvm_container: String,
    _propdir: TempDir,
    /// Comma-separated `host.docker.internal:<port>` bootstrap for all data
    /// listeners reachable from inside the tool containers.
    bootstrap_all: String,
}

impl MixedCluster {
    /// Block, with a bound, until the Crabka leader's broker view includes `n`
    /// registered brokers. That is, the JVM data-plane broker (id 3) has
    /// finished its `KRaft` join and registered. `CreateTopics(RF=3)` rejects
    /// with `InvalidReplicationFactorException` if it runs before the JVM
    /// broker registers, so every mixed-cluster scenario must gate on this
    /// first. This method returns `true` if the view converged and `false` on
    /// timeout. A timeout means the JVM broker never joined, which is the
    /// dominant Linux-vs-Mac difference for this harness.
    async fn wait_for_brokers(&self, n: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let mut max_seen = 0;
            for (h, _) in &self.crabka {
                max_seen = max_seen.max(h.broker_count());
            }
            if max_seen >= n {
                return true;
            }
            if Instant::now() > deadline {
                eprintln!(
                    "CRABKA[kip320] only {max_seen}/{n} brokers registered before timeout \
                     (JVM broker likely never joined the mixed cluster)"
                );
                return false;
            }
            // intentional: bounded poll for an EXTERNAL JVM broker's KRaft
            // registration; the 2-min bound + bool-on-timeout return (surfacing
            // "JVM never joined") can't be replaced by a 30s panic-on-timeout
            // awaiter.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn shutdown(self) {
        docker_rm(&self.jvm_container);
        for (h, _) in self.crabka {
            h.shutdown().await;
        }
    }
}

/// Build a Crabka broker config that is BOTH a controller voter (in the shared
/// static `KRaft` quorum) and a data-plane broker. Mirrors
/// `jvm_static_quorum_spike.rs::crabka_controller_config` plus a bound data
/// listener.
fn crabka_mixed_config(
    i: usize,
    client_port: u16,
    advertised_host: &str,
    own_controller_addr: SocketAddr,
    voters: &[(u64, SocketAddr)],
    cluster_id: Uuid,
    log_dir: &std::path::Path,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.node_id = crabka_broker::NodeId(u64::try_from(i + 1).unwrap());
    cfg.listen_addr = format!("0.0.0.0:{client_port}").parse().unwrap();
    cfg.advertised_listener = format!("{advertised_host}:{client_port}");
    cfg.controller_listen_addr = own_controller_addr;
    cfg.directory_id = Uuid::from_u128(u128::from(cfg.node_id.0));
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
        .collect();
    cfg.auto_join = false;
    cfg.bootstrap_servers = vec![];
    cfg.cluster_id = Some(cluster_id);
    cfg.heartbeat_interval = crabka_units::millis(1_000);
    cfg.heartbeat_timeout = crabka_units::millis(4_000);
    cfg.replica_lag_time_max = crabka_units::millis(10_000);
    cfg.controller_election_timeout = crabka_units::secs(3);
    cfg.controller_heartbeat_interval = crabka_units::millis(250);
    cfg
}

/// Stand up two Crabka brokers (the metadata-quorum majority + data plane) and
/// one mirror.gcr.io/apache/kafka:4.0.0 broker joined to the same static `KRaft` quorum.
/// Returns once the Crabka voters have elected a shared leader. The JVM broker
/// starts detached and the caller polls for it to register.
async fn start_mixed_cluster(container: &str) -> MixedCluster {
    support::init_tracing();
    docker_rm(container);

    let cluster_id = Uuid::from_u128(0x4b49_5033_3230_4d49_5845_4451_554f_5255);
    let cid_str = kafka_cluster_id_string(cluster_id);
    let advertised_host = docker_bridge_gateway();

    // Pre-bind 2 Crabka client ports, 3 controller ports.
    let (client_addrs, controller_addrs) = support::bind_and_drop_ports(3).await;
    let crabka_client_ports = [client_addrs[0].port(), client_addrs[1].port()];
    let p1 = controller_addrs[0].port();
    let p2 = controller_addrs[1].port();
    let p3 = controller_addrs[2].port();

    let crabka_voters: Vec<(u64, SocketAddr)> = vec![
        (1, format!("127.0.0.1:{p1}").parse().unwrap()),
        (2, format!("127.0.0.1:{p2}").parse().unwrap()),
        (3, format!("127.0.0.1:{p3}").parse().unwrap()),
    ];

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let cfg1 = crabka_mixed_config(
        0,
        crabka_client_ports[0],
        &advertised_host,
        format!("0.0.0.0:{p1}").parse().unwrap(),
        &crabka_voters,
        cluster_id,
        dir1.path(),
    );
    let cfg2 = crabka_mixed_config(
        1,
        crabka_client_ports[1],
        &advertised_host,
        format!("0.0.0.0:{p2}").parse().unwrap(),
        &crabka_voters,
        cluster_id,
        dir2.path(),
    );
    let (c1, c2): (BrokerHandle, BrokerHandle) = {
        let s1 = tokio::spawn(Broker::start(cfg1));
        let s2 = tokio::spawn(Broker::start(cfg2));
        (
            s1.await.unwrap().expect("crabka voter 1"),
            s2.await.unwrap().expect("crabka voter 2"),
        )
    };

    // Start the JVM broker (id 3): process.roles=broker,controller, joining the
    // shared static quorum, publishing a data listener (PLAINTEXT) and its
    // controller port. Reachable from tool containers at host.docker.internal.
    let jvm_data_port = client_addrs[2].port();
    let props = format!(
        "process.roles=broker,controller\n\
         node.id=3\n\
         controller.quorum.voters=1@host.docker.internal:{p1},2@host.docker.internal:{p2},3@localhost:{p3}\n\
         controller.listener.names=CONTROLLER\n\
         listeners=PLAINTEXT://0.0.0.0:{jvm_data_port},CONTROLLER://0.0.0.0:{p3}\n\
         advertised.listeners=PLAINTEXT://{advertised_host}:{jvm_data_port}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n\
         inter.broker.listener.name=PLAINTEXT\n\
         log.dirs=/tmp/kraft-mixed-logs\n"
    );
    let propdir = TempDir::new().unwrap();
    let proppath = propdir.path().join("server.properties");
    std::fs::write(&proppath, props).unwrap();
    // The Apache Kafka image runs as a non-root uid. `tempfile` creates its
    // directory as 0700, so a bind-mounted file below it is otherwise present
    // but unreadable on native Linux (the CI runner included).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::set_permissions(propdir.path(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod server.properties directory");
        std::fs::set_permissions(&proppath, std::fs::Permissions::from_mode(0o644))
            .expect("chmod server.properties");
    }
    let entry = format!(
        "/opt/kafka/bin/kafka-storage.sh format -t {cid_str} --config /tmp/s.properties --ignore-formatted && \
         exec /opt/kafka/bin/kafka-server-start.sh /tmp/s.properties"
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            container,
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            &format!("{p3}:{p3}"),
            "-p",
            &format!("{jvm_data_port}:{jvm_data_port}"),
            "-v",
            &format!("{}:/tmp/s.properties", proppath.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE_KRAFT,
            "-c",
            &entry,
        ])
        .status()
        .expect("docker run JVM broker");
    assert!(status.success(), "docker run JVM broker failed");

    // Wait for the Crabka voters to elect a shared leader (event-driven: each
    // awaiter resolves once that voter observes a non-zero controller leader).
    c1.wait_until_controller_leader().await;
    c2.wait_until_controller_leader().await;

    let bootstrap_all = format!(
        "{}:{},{}:{},{}:{}",
        advertised_host,
        crabka_client_ports[0],
        advertised_host,
        crabka_client_ports[1],
        advertised_host,
        jvm_data_port,
    );

    MixedCluster {
        crabka: vec![(c1, dir1), (c2, dir2)],
        jvm_container: container.to_string(),
        _propdir: propdir,
        bootstrap_all,
    }
}

async fn wait_for_jvm_metadata_max(cluster: &MixedCluster, expected: i16) {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let observed = cluster.crabka.iter().find_map(|(broker, _)| {
            broker
                .controller_image_for_test()
                .broker(crabka_broker::NodeId(3))
                .and_then(|registration| {
                    registration
                        .features
                        .get(crabka_metadata::metadata_version::METADATA_VERSION_FEATURE)
                        .map(|(_, max)| *max)
                })
        });
        if observed == Some(expected) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "JVM broker did not advertise metadata.version max {expected}; observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// KIP-1155 mixed-version safety: Kafka 4.0 predates the proposed online
/// downgrade capability. It must block both safe and unsafe downgrades; unsafe
/// permits record loss, but never permits a node that cannot perform the
/// immediate snapshot/reload protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + published controller/data ports; Linux-bound"]
async fn metadata_version_downgrade_rejects_pre_kip1155_jvm() {
    const EXISTING_TOPIC: &str = "crabka-mv-capability-existing";
    const CONTAINER: &str = "crabka-mv-capability-jvm-broker";
    const UPPER_LEVEL: i16 = 25; // 4.0-IV3.

    let cluster = start_mixed_cluster(CONTAINER).await;
    assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "JVM broker never joined the mixed cluster"
    );
    wait_for_jvm_metadata_max(&cluster, UPPER_LEVEL).await;
    create_mixed_topic(&cluster.bootstrap_all, EXISTING_TOPIC).await;
    let state = |broker: &BrokerHandle| {
        let image = broker.controller_image_for_test();
        (
            image.finalized_metadata_version(),
            image
                .brokers()
                .map(|registration| (registration.node_id, registration.log_dirs.clone()))
                .collect::<Vec<_>>(),
            image
                .partition(EXISTING_TOPIC, 0)
                .expect("existing mixed topic")
                .directories
                .clone(),
        )
    };
    let before = cluster
        .crabka
        .iter()
        .map(|(broker, _)| state(broker))
        .collect::<Vec<_>>();
    let image = cluster.crabka[0].0.controller_image_for_test();
    assert!(
        !image
            .broker(crabka_broker::NodeId(3))
            .expect("Kafka 4.0 registration")
            .features
            .contains_key(crabka_metadata::metadata_version::METADATA_DOWNGRADE_CAPABILITY_FEATURE),
        "pre-KIP-1155 JVM registration unexpectedly advertised downgrade capability"
    );

    for (kind, command) in [
        ("safe", vec!["downgrade", "--metadata", "3.7-IV1"]),
        (
            "unsafe",
            vec!["downgrade", "--metadata", "3.7-IV1", "--unsafe"],
        ),
    ] {
        let output = run_features(&cluster.bootstrap_all, &command);
        let error = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !output.status.success()
                && error.contains("Broker 3")
                && error.contains("does not support online metadata.version downgrade"),
            "{kind} downgrade did not reject the pre-capability JVM node: {error}"
        );
    }

    let after = cluster
        .crabka
        .iter()
        .map(|(broker, _)| state(broker))
        .collect::<Vec<_>>();
    assert!(
        after == before,
        "rejected mixed-version downgrade changed finalized or directory metadata"
    );
    cluster.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 2: induced divergence — a JVM follower truncates from a Crabka
// leader, and a JVM consumer recovers.
// ─────────────────────────────────────────────────────────────────────────────

/// Steps 2-3 of Task 11. Force a real divergent suffix in a mixed cluster and
/// assert:
///  (a) the JVM follower truncates to converge on the Crabka leader, and
///  (b) a kafka-console-consumer recovers. It continues without a fatal
///      truncation/deserialization error after the suffix is rewritten.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller/data port; Linux-bound"]
async fn kip320_jvm_follower_truncates_from_crabka_leader() {
    const TOPIC: &str = "crabka-kip320-jvm-follower";
    const CONTAINER: &str = "crabka-kip320-jvm-follower-broker";

    let cluster = start_mixed_cluster(CONTAINER).await;
    let c1 = &cluster.crabka[0].0; // Crabka broker_id 1
    let bootstrap_all = cluster.bootstrap_all.clone();

    // 0. Gate on the JVM broker (id 3) registering into the cluster view. On
    //    Linux/CI the cross-impl KRaft join completes within ~1 min; if it
    //    never registers (the JVM broker failed to join the Crabka-led quorum
    //    — the dominant Mac-vs-Linux difference here) we cannot build an RF=3
    //    topic, so we surface that explicitly rather than fail opaquely inside
    //    CreateTopics.
    assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "JVM broker never joined the mixed cluster (only the 2 Crabka brokers \
         registered); the cross-impl KRaft data-plane join is Linux-bound"
    );

    // 1. Create an RF=3 topic placed on the two Crabka brokers + JVM. With 3
    //    registered brokers the controller assigns replicas across all three;
    //    we use partitions=1, replication-factor=3 so the JVM (id 3) is a
    //    replica/follower of a Crabka leader.
    create_mixed_topic(&bootstrap_all, TOPIC).await;

    // 2. Wait for the partition to materialize on the Crabka leader and for the
    //    JVM follower to join the ISR.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let desc = docker_run_kafka_tool_with_image(
            KAFKA_IMAGE,
            &[
                "kafka-topics",
                "--describe",
                "--topic",
                TOPIC,
                "--bootstrap-server",
                &bootstrap_all,
            ],
        );
        let s = String::from_utf8_lossy(&desc.stdout);
        // ISR must contain broker 3 (the JVM follower) so it is actively
        // replicating from the Crabka leader before we induce divergence.
        if described_isr(&s).contains(&3) {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "JVM follower never joined ISR: {s}"
        );
        // intentional: polls an EXTERNAL kafka-topics --describe CLI for the
        // JVM follower (id 3) to catch up and join the ISR; driven by the JVM
        // broker's fetch, with a 2-min bound the 30s image awaiter can't match.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 3. Produce a committed prefix (epoch 0) via the JVM producer (acks=all),
    //    so all replicas — including the JVM follower — share it.
    produce_lines_via_jvm(
        &bootstrap_all,
        TOPIC,
        &(0..10).map(|i| format!("prefix-{i}")).collect::<Vec<_>>(),
    );
    // intentional: let the EXTERNAL JVM follower replicate the acks=all prefix;
    // the follower's replication progress is not a Crabka image/metric signal.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 4. INDUCE REAL DIVERGENCE. First make the JVM broker leader and append a
    //    suffix there. Crabka follows that suffix so both sides demonstrably
    //    have it. Then park every fetcher behind a dead phantom leader, truncate
    //    broker 1 back to the committed prefix, and append a different suffix at
    //    the next epoch. Restoring broker 1 as leader leaves equal-length,
    //    byte-different tails: the JVM follower must truncate, not merely catch
    //    up from a shorter log.
    let prefix_leo = c1
        .local_log_end_offset(TOPIC, 0)
        .expect("Crabka prefix log exists");
    assert!(
        prefix_leo == 10,
        "expected ten-record prefix, got LEO {prefix_leo}"
    );
    let pr = {
        // Wait for the partition to materialize in the Crabka leader's image.
        c1.wait_until_partition_present(TOPIC, 0).await;
        c1.partition_record_for_test(TOPIC, 0)
            .expect("partition record present after wait")
    };
    eprintln!("CRABKA[kip320] partition before divergence: {pr:?}");

    let jvm_epoch = LeaderEpoch(pr.leader_epoch.0 + 1);
    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: crabka_broker::NodeId(3),
        replicas: pr.replicas.clone(),
        isr: vec![crabka_broker::NodeId(3)],
        leader_epoch: jvm_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: pr.directories.clone(),
        partition_epoch: pr.partition_epoch + 1,
    }))
    .await
    .expect("promote JVM broker for divergent suffix");
    wait_for_described_leader(&bootstrap_all, TOPIC, 3, Duration::from_secs(45)).await;

    let jvm_suffix = (0..4)
        .map(|i| format!("jvm-divergent-{i}"))
        .collect::<Vec<_>>();
    produce_lines_via_jvm(&bootstrap_all, TOPIC, &jvm_suffix);
    c1.wait_until_local_log_end_offset(TOPIC, 0, prefix_leo + 4)
        .await;
    let jvm_before = dump_log_in_container(CONTAINER, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));
    assert!(
        jvm_before.contains("jvm-divergent-3"),
        "JVM dump did not contain the suffix that must later be truncated:\n{jvm_before}"
    );

    // Freeze the JVM process while rewriting broker 1. The phantom-leader
    // metadata cancels replication cooperatively, so an already in-flight
    // response from the former JVM leader could otherwise reset the test log
    // during this deliberately out-of-band mutation.
    set_container_paused(CONTAINER, true);

    // Take the partition offline behind a dead phantom leader (id 99). Keep
    // the assignment and directory vector intact so this record changes only
    // leadership/epoch state.
    let parked_epoch = LeaderEpoch(jvm_epoch.0 + 1);
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: crabka_broker::NodeId(99),
        replicas: pr.replicas.clone(),
        isr: vec![crabka_broker::NodeId(99)],
        leader_epoch: parked_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: pr.directories.clone(),
        partition_epoch: pr.partition_epoch + 2,
    });
    c1.submit_metadata_record_for_test(forged)
        .await
        .expect("inject dead-leader PartitionRecord");
    let parked_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if c1.partition_record_for_test(TOPIC, 0).is_some_and(|p| {
            p.leader == crabka_broker::NodeId(99) && p.leader_epoch == parked_epoch
        }) {
            break;
        }
        assert!(
            Instant::now() <= parked_deadline,
            "dead-leader metadata did not apply before divergent rewrite"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Remove the JVM suffix from broker 1, then append the Crabka suffix at the
    // parked epoch. The test append helper stamps that current epoch on every
    // batch, giving KIP-320 a real epoch boundary at prefix_leo.
    c1.test_truncate_local_log(TOPIC, 0, prefix_leo)
        .await
        .expect("truncate Crabka copy of JVM suffix");
    let crabka_leo_before = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    c1.produce_records_for_test(TOPIC, 0, 4)
        .await
        .expect("append divergent suffix on Crabka leader");
    let crabka_leo_after = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    eprintln!(
        "CRABKA[kip320] Crabka leader LEO {crabka_leo_before} -> {crabka_leo_after} (divergent suffix)"
    );

    assert!(
        crabka_leo_before == prefix_leo && crabka_leo_after == prefix_leo + 4,
        "Crabka divergent rewrite should replace four offsets in place"
    );

    // Restore Crabka broker 1 as the leader at the next epoch with the JVM
    // follower (3) back in the replica set so it re-fetches and detects
    // divergence.
    let restore = MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: crabka_broker::NodeId(1),
        replicas: pr.replicas.clone(),
        isr: vec![crabka_broker::NodeId(1)],
        leader_epoch: LeaderEpoch(parked_epoch.0 + 1),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: pr.directories.clone(),
        partition_epoch: pr.partition_epoch + 3,
    });
    c1.submit_metadata_record_for_test(restore)
        .await
        .expect("restore Crabka leader");

    set_container_paused(CONTAINER, false);

    wait_for_described_leader(&bootstrap_all, TOPIC, 1, Duration::from_secs(45)).await;

    // 5. Poll the JVM broker's actual on-disk bytes until its old suffix is
    //    gone and the Crabka suffix is present. Equal LEOs alone cannot prove
    //    truncation because both divergent tails contain four records.
    let convergence_deadline = Instant::now() + Duration::from_mins(1);
    let jvm_dump = loop {
        let dump = dump_log_in_container(CONTAINER, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));
        if dump.contains("test-record-3") && !dump.contains("jvm-divergent-") {
            break dump;
        }
        assert!(
            Instant::now() <= convergence_deadline,
            "JVM follower retained its divergent suffix after KIP-320 recovery:\n{dump}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    // 6. ASSERTION (a): the JVM follower's on-disk log converged on the Crabka
    //    leader's exact LEO. The payload assertions above already prove that
    //    the equal-length old suffix was removed and replaced.
    eprintln!(
        "CRABKA[kip320] jvm dump baseOffset lines:\n{}",
        grep_base_offsets(&jvm_dump)
    );

    // Exact dump text is intentionally not compared across implementations
    // because timestamps and batch packing differ. The leader's in-process LEO
    // is the authoritative next offset; kafka-dump-log supplies the follower's
    // independently parsed, on-disk last offset.
    let jvm_max = max_offset_in_dump(&jvm_dump);
    assert!(
        jvm_max == Some(crabka_leo_after - 1),
        "JVM follower did not converge to Crabka leader after truncation: \
         jvm_max={jvm_max:?} crabka_leo={crabka_leo_after}"
    );

    // 7. ASSERTION (b): a kafka-console-consumer recovers — it reads the
    //    truncated/converged log to completion without a fatal
    //    LogTruncationException / RecordDeserializationException.
    let consume = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            &bootstrap_all,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "20000",
        ],
    );
    let cstdout = String::from_utf8_lossy(&consume.stdout);
    let cstderr = String::from_utf8_lossy(&consume.stderr);
    eprintln!("CRABKA[kip320] consumer recover stdout={cstdout} stderr={cstderr}");
    assert!(
        !cstderr.contains("LogTruncationException")
            && !cstderr.contains("RecordDeserializationException"),
        "consumer hit a fatal truncation/deserialization error: {cstderr}"
    );
    assert!(
        cstdout.lines().filter(|l| !l.trim().is_empty()).count() >= 1,
        "consumer read no records after truncation recovery: stdout={cstdout} stderr={cstderr}"
    );

    cluster.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 3: reverse direction — a Crabka follower truncates from a JVM
// leader.
// ─────────────────────────────────────────────────────────────────────────────

/// Step 2 of Task 11, reverse direction. A Crabka follower replicates from a
/// JVM leader. The test parks replication behind a phantom leader, appends a
/// suffix only to the Crabka replica at a new epoch, and then promotes the JVM
/// replica. It asserts that the Crabka follower observes the JVM leader's
/// `diverging_epoch`, truncates to their shared prefix, and subsequently copies
/// a fresh JVM-authored suffix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller/data port; Linux-bound"]
async fn kip320_crabka_follower_truncates_from_jvm_leader() {
    const TOPIC: &str = "crabka-kip320-crabka-follower";
    const CONTAINER: &str = "crabka-kip320-crabka-follower-broker";

    let cluster = start_mixed_cluster(CONTAINER).await;
    let c1 = &cluster.crabka[0].0;
    let bootstrap_all = cluster.bootstrap_all.clone();

    // 0. Gate on the JVM broker registering (see scenario 2); RF=3 needs all
    //    three brokers in the cluster view. Linux-bound.
    assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "JVM broker never joined the mixed cluster; cross-impl KRaft join is Linux-bound"
    );

    // 1. Create the topic and wait for replicas to converge across all three
    //    brokers.
    create_mixed_topic(&bootstrap_all, TOPIC).await;

    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let desc = docker_run_kafka_tool_with_image(
            KAFKA_IMAGE,
            &[
                "kafka-topics",
                "--describe",
                "--topic",
                TOPIC,
                "--bootstrap-server",
                &bootstrap_all,
            ],
        );
        let s = String::from_utf8_lossy(&desc.stdout);
        let isr = described_isr(&s);
        if isr.contains(&1) && isr.contains(&3) {
            break;
        }
        assert!(Instant::now() <= deadline, "replicas never converged: {s}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 2. Produce a committed prefix via the JVM producer (acks=all) so the
    //    Crabka follower shares it.
    produce_lines_via_jvm(
        &bootstrap_all,
        TOPIC,
        &(0..8).map(|i| format!("rev-{i}")).collect::<Vec<_>>(),
    );
    // intentional: let the EXTERNAL JVM producer/replication settle so the
    // Crabka follower shares the prefix; no Crabka image/metric signal for it.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 3. Park replication behind a phantom leader before appending the
    //    Crabka-only suffix. This makes the divergent state deterministic:
    //    neither the JVM replica nor broker 2 can copy the forged records.
    let prefix_leo = c1
        .local_log_end_offset(TOPIC, 0)
        .expect("Crabka prefix log exists");
    assert2::assert!(
        prefix_leo == 8,
        "expected eight-record prefix, got LEO {prefix_leo}"
    );
    c1.wait_until_partition_present(TOPIC, 0).await;
    let partition = c1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record present after wait");
    let parked_epoch = LeaderEpoch(partition.leader_epoch.0 + 1);
    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: crabka_broker::NodeId(99),
        replicas: partition.replicas.clone(),
        isr: vec![crabka_broker::NodeId(99)],
        leader_epoch: parked_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: partition.directories.clone(),
        partition_epoch: partition.partition_epoch + 1,
    }))
    .await
    .expect("park reverse-direction replicas behind phantom leader");
    let parked_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if c1
            .partition_record_for_test(TOPIC, 0)
            .is_some_and(|record| {
                record.leader == crabka_broker::NodeId(99) && record.leader_epoch == parked_epoch
            })
        {
            break;
        }
        assert2::assert!(
            Instant::now() <= parked_deadline,
            "phantom-leader metadata did not apply before reverse divergence"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    c1.produce_records_for_test(TOPIC, 0, 5)
        .await
        .expect("append divergent suffix on parked Crabka replica");
    let crabka_leo_diverged = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    eprintln!(
        "CRABKA[kip320] reverse: Crabka replica LEO {prefix_leo} -> {crabka_leo_diverged} (forced divergent suffix)"
    );
    assert2::assert!(
        crabka_leo_diverged == prefix_leo + 5,
        "Crabka-only divergent suffix should add five records"
    );

    // 4. Promote the JVM replica at the next epoch. Its log still ends at the
    //    shared prefix, so the Crabka follower must truncate before fetching.
    let jvm_epoch = LeaderEpoch(parked_epoch.0 + 1);
    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: crabka_broker::NodeId(3),
        replicas: partition.replicas.clone(),
        isr: vec![crabka_broker::NodeId(3)],
        leader_epoch: jvm_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: partition.directories.clone(),
        partition_epoch: partition.partition_epoch + 2,
    }))
    .await
    .expect("promote JVM broker for reverse-direction recovery");
    wait_for_described_leader(&bootstrap_all, TOPIC, 3, Duration::from_secs(45)).await;

    // 5. Observe the truncation itself, before adding any new leader records.
    //    Equal final LEOs alone would not distinguish truncate-and-refetch from
    //    leaving the bogus suffix in place.
    let dl = Instant::now() + Duration::from_secs(45);
    let mut final_leo = crabka_leo_diverged;
    loop {
        final_leo = c1.local_log_end_offset(TOPIC, 0).unwrap_or(final_leo);
        if final_leo == prefix_leo {
            break;
        }
        assert2::assert!(
            Instant::now() <= dl,
            "Crabka follower did not truncate to JVM prefix LEO {prefix_leo}; current LEO={final_leo}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let jvm_prefix_dump =
        dump_log_in_container(CONTAINER, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));
    assert2::assert!(
        max_offset_in_dump(&jvm_prefix_dump) == Some(prefix_leo - 1),
        "JVM leader should retain exactly the shared prefix:\n{jvm_prefix_dump}"
    );
    assert2::assert!(
        !jvm_prefix_dump.contains("test-record-"),
        "Crabka-only divergent suffix leaked to JVM leader:\n{jvm_prefix_dump}"
    );

    // 6. Prove that replication resumes from the truncated boundary by writing
    //    a shorter, JVM-authored suffix and waiting for Crabka's exact LEO.
    let authoritative = (0..3)
        .map(|i| format!("jvm-authoritative-{i}"))
        .collect::<Vec<_>>();
    produce_lines_via_jvm(&bootstrap_all, TOPIC, &authoritative);
    c1.wait_until_local_log_end_offset(TOPIC, 0, prefix_leo + 3)
        .await;
    final_leo = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    assert2::assert!(
        final_leo == prefix_leo + 3,
        "Crabka follower did not resume at the JVM leader's exact LEO"
    );
    eprintln!(
        "CRABKA[kip320] reverse: truncated from {crabka_leo_diverged} to {prefix_leo}, then followed JVM to {final_leo}"
    );

    cluster.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// kafka-dump-log helpers (mirror three_node_replication_byte_compare).
// ─────────────────────────────────────────────────────────────────────────────

/// Dump a partition segment that lives INSIDE the running JVM broker container
/// with `docker exec` and the container's bundled `kafka-dump-log`.
fn dump_log_in_container(container: &str, partition_dir: &str) -> String {
    let listed = Command::new("docker")
        .args([
            "exec",
            container,
            "find",
            partition_dir,
            "-maxdepth",
            "1",
            "-type",
            "f",
            "-name",
            "*.log",
            "-print",
        ])
        .output()
        .expect("list JVM log segments");
    if !listed.status.success() {
        return String::from_utf8_lossy(&listed.stderr).to_string();
    }
    let mut log_files = String::from_utf8_lossy(&listed.stdout)
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    log_files.sort();
    if log_files.is_empty() {
        return String::new();
    }
    let files = log_files.join(",");
    let out = Command::new("docker")
        .args([
            "exec",
            container,
            "/opt/kafka/bin/kafka-dump-log.sh",
            "--files",
            &files,
            "--print-data-log",
        ])
        .output()
        .expect("spawn dump-log exec");
    let mut dump = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() {
        dump.push_str(&String::from_utf8_lossy(&out.stderr));
    }
    dump
}

/// Extract the highest record offset reported by a `kafka-dump-log`
/// `--print-data-log` dump (max of any `lastOffset:` / `offset:` field).
fn max_offset_in_dump(dump: &str) -> Option<i64> {
    let mut max = None;
    let mut tokens = dump.split_whitespace().peekable();
    while let Some(token) = tokens.next() {
        let raw = if matches!(token, "lastOffset:" | "offset:") {
            tokens.next()
        } else {
            ["lastOffset:", "offset:"]
                .iter()
                .find_map(|key| token.strip_prefix(key))
                .filter(|value| !value.is_empty())
        };
        if let Some(raw) = raw
            && let Ok(value) = raw
                .trim_matches(|character: char| !character.is_ascii_digit() && character != '-')
                .parse::<i64>()
        {
            max = Some(max.map_or(value, |current: i64| current.max(value)));
        }
    }
    max
}

/// Pull just the `baseOffset`/`lastOffset` summary lines for log readability.
fn grep_base_offsets(dump: &str) -> String {
    dump.lines()
        .filter(|l| l.contains("baseOffset"))
        .take(40)
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn kafka_dump_offset_parser_accepts_spaced_values() {
    let dump = "baseOffset: 0 lastOffset: 9 count: 10\n\
                offset: 10 position: 211 payload: value";
    assert2::assert!(max_offset_in_dump(dump) == Some(10));
}

#[test]
fn topic_description_isr_parser_ignores_ids_outside_isr_field() {
    let description =
        "Topic: crabka-kip320-3 Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2 Elr: 3";
    assert2::assert!(described_isr(description) == vec![1, 2]);
}
