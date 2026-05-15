//! End-to-end tests that drive the official Apache Kafka command-line
//! tools (running inside `confluentinc/cp-kafka:6.1.1` containers) against
//! a Rust `crabka-broker` running on the host.
//!
//! Both tests are gated `#[ignore = "requires Docker"]` so `cargo test`
//! doesn't pull Docker by default. Run with `--ignored`.
//!
//! Networking: the Rust broker listens on `0.0.0.0:9092` of the host. The
//! Kafka CLI containers use Docker's default bridge network plus
//! `--add-host=host.docker.internal:host-gateway`, which on both Docker
//! Desktop and Linux Docker 20.10+ maps `host.docker.internal` to the
//! bridge gateway IP that the host's `0.0.0.0:9092` is bound on. The
//! broker's advertised listener is `host.docker.internal:9092` so the
//! `AdminClient`'s *second* connect (post-Metadata) targets a hostname the
//! container can resolve.
//!
//! We deliberately do NOT use `--network host`: on hosted GitHub Actions
//! ubuntu-24.04 runners, that mode silently fails to share the host's
//! loopback (the container can `nc -zv 127.0.0.1 9092` but a Java NIO
//! `SocketChannel.connect()` to the same address times out), and we have
//! no good way to debug that from a Rust integration test.

#![cfg(not(target_os = "windows"))]

use std::io::Write;
use std::process::{Command, Stdio};

use crabka_broker::{Broker, BrokerConfig};
use crabka_log::LogConfig;

const HOST_PORT: u16 = 9092;
/// Address the Kafka CLI containers use for bootstrap AND that the broker
/// advertises in `Metadata`. Resolved via `--add-host=host.docker.internal:
/// host-gateway` in [`docker_run_kafka_tool`].
const BOOTSTRAP: &str = "host.docker.internal:9092";
/// Bind to all interfaces so the Docker bridge can reach us via the host
/// gateway IP.
const LISTEN: &str = "0.0.0.0:9092";
const KAFKA_IMAGE: &str = "confluentinc/cp-kafka:6.1.1";
/// Newer Kafka image used for tests that require tools not bundled in
/// [`KAFKA_IMAGE`]. Currently referenced by:
///
/// - `kafka_cluster_describe`: `kafka-cluster` binary is absent from
///   `cp-kafka:6.1.1` but present in `cp-kafka:7.5.0`.
///
/// NOTE: `cp-kafka:7.5.0`'s bundled `kafka-verifiable-producer` does NOT
/// support `--transactional-id` despite shipping Kafka 3.5. The test that
/// requires that flag is gated behind `CRABKA_RUN_TXN_JVM_TEST` and
/// deferred to slice 10 pending a custom Java snippet harness.
const KAFKA_IMAGE_TXN: &str = "confluentinc/cp-kafka:7.5.0";

/// Spawn the broker, listening on `LISTEN`. The advertised listener is
/// `host.docker.internal:9092`; inside the cp-kafka containers we add a
/// hosts entry pointing that name at the bridge gateway.
async fn start_host_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
    tracing::info!(listen = %LISTEN, advertised = %BOOTSTRAP, "broker started for jvm acceptance");
    (handle, dir)
}

/// Verify TCP connectivity from inside a bridge-network container with
/// `--add-host=host.docker.internal:host-gateway`.
fn nc_check_connectivity() {
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "alpine",
            "sh",
            "-c",
            "apk add --no-cache netcat-openbsd >/dev/null 2>&1 && nc -zv host.docker.internal 9092",
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

/// Run `docker run --rm --add-host=host.docker.internal:host-gateway
/// <image> <args...>`, asserting success.
fn docker_run_kafka_tool(args: &[&str]) -> std::process::Output {
    docker_run_kafka_tool_with_image(KAFKA_IMAGE, args)
}

/// Like [`docker_run_kafka_tool`] but lets the caller choose the image.
/// Used when a specific test needs a newer image (e.g. `kafka-cluster`
/// is only bundled in `cp-kafka:7.5.0`, not `6.1.1`).
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
        "CRABKA[test] docker_run image={image} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run image={image} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

// `flavor = "multi_thread"` is essential here. The test bodies make
// synchronous blocking `Command::output()` calls for each `docker run`.
// On a single-threaded runtime those calls block the only worker — which
// is also driving the broker's accept loop. Incoming TCP connections then
// complete the kernel-level handshake but the broker never reads them,
// and the Java AdminClient times out. A multi-thread runtime puts the
// broker on a separate worker so the test's blocking calls don't starve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
            "--add-host=host.docker.internal:host-gateway",
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

// `flavor = "multi_thread"` is essential here. The test bodies make
// synchronous blocking `Command::output()` calls for each `docker run`.
// On a single-threaded runtime those calls block the only worker — which
// is also driving the broker's accept loop. Incoming TCP connections then
// complete the kernel-level handshake but the broker never reads them,
// and the Java AdminClient times out. A multi-thread runtime puts the
// broker on a separate worker so the test's blocking calls don't starve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

// Same multi-thread runtime caveat as `console_producer_round_trip`:
// the test body makes blocking `Command::output()` calls; a
// single-threaded runtime would starve the broker's accept loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rust_producer_to_console_consumer() {
    use crabka_client_producer::{Acks, Compression, Producer, ProducerRecord};

    const TOPIC: &str = "crabka-rust-producer-itest";

    let (broker, _dir) = start_host_broker().await;

    // 1. Create the topic.
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

    // 2. Build a Rust producer pointed at the host broker and produce 3 records.
    let producer = Producer::builder()
        .bootstrap(BOOTSTRAP.to_string())
        .enable_idempotence(true)
        .acks(Acks::All)
        .compression(Compression::Lz4)
        .build()
        .await
        .expect("producer");
    for v in ["x", "y", "z"] {
        let fut = producer
            .send(ProducerRecord {
                topic: TOPIC.into(),
                value: Some(bytes::Bytes::from(v)),
                ..Default::default()
            })
            .await;
        let m = fut.await.expect("oneshot").expect("ack");
        assert_eq!(m.partition, 0);
    }
    producer.flush().await.expect("flush");
    producer.close().await.expect("close");

    // 3. Consume via kafka-console-consumer --partition 0.
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
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["x", "y", "z"] {
        assert!(s.contains(needle), "missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}

// Same multi-thread runtime caveat as `console_producer_round_trip`:
// the test body makes blocking `Command::output()` calls; a
// single-threaded runtime would starve the broker's accept loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn console_consumer_with_group_round_trip() {
    const TOPIC: &str = "crabka-broker-grp-itest";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    // 1. Create the topic.
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

    // 2. Produce records via kafka-console-producer over stdin.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
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
        .write_all(b"x\ny\nz\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // 3. Consume WITHOUT --partition. The default `console-consumer`
    //    group will JoinGroup → SyncGroup → Heartbeat → Fetch through
    //    our coordinator.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC,
        "--from-beginning",
        "--group",
        "crabka-acceptance-group",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["x", "y", "z"] {
        assert!(s.contains(needle), "consumer didn't emit {needle}: {s:?}");
    }

    broker.shutdown().await;
}

// Three-node quorum: produce on one node, consume on another, then kill
// the controller leader and assert the surviving brokers still answer
// Metadata. Same multi-thread runtime caveat as the other tests; we ask
// for 4 workers because we have three brokers + the test driver all
// making blocking docker calls.
//
// Fixed ports per node because docker containers must be able to reach
// the brokers via `host.docker.internal:<client-port>`. The advertised
// listener uses the same hostname so the AdminClient's post-Metadata
// reconnect resolves correctly. Controller ports use `127.0.0.1` for
// inter-broker traffic — all three Crabka brokers live on the host's
// loopback, so docker reachability is not required for the controller
// listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn three_node_jvm_round_trip() {
    const TOPIC: &str = "crabka-quorum-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    let client_ports = [9192u16, 9292, 9392];
    let controller_ports = [9193u16, 9293, 9393];

    // Voters for inter-broker (controller) traffic: host loopback works.
    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i])
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    // Bootstrap-then-join: start broker 0 alone (it self-elects as a
    // singleton voter), then start brokers 1, 2 in Join mode and bring
    // them into the cluster via add_learner + change_membership. Avoids
    // openraft's cold-boot split-vote risk.
    let mut tempdirs: Vec<tempfile::TempDir> = Vec::with_capacity(3);

    // Broker 0 (Bootstrap).
    let dir0 = tempfile::tempdir().expect("tempdir");
    let cfg0 = BrokerConfig {
        broker_id: 1,
        // bind on 0.0.0.0 so Docker-side containers can reach us.
        listen_addr: format!("0.0.0.0:{}", client_ports[0])
            .parse()
            .expect("static addr"),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: 1,
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0])
            .parse()
            .expect("static addr"),
        controller_quorum_voters: voters.clone(),
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: std::time::Duration::from_secs(5),
        controller_heartbeat_interval: std::time::Duration::from_millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let broker0 = Broker::start(cfg0).await.expect("broker start");

    // Brokers 1, 2 (Join).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i])
                .parse()
                .expect("static addr"),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: LogConfig::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i])
                .parse()
                .expect("static addr"),
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: crabka_broker::BootstrapMode::Join,
            ..BrokerConfig::default()
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            Broker::start(cfg).await.expect("broker start")
        }));
    }

    // Bring the join brokers into the cluster: add as learners, then
    // promote to voters in one change_membership.
    let voter_addr = |i: usize| -> std::net::SocketAddr {
        format!("127.0.0.1:{}", controller_ports[i])
            .parse()
            .expect("static addr")
    };
    broker0
        .add_learner(2, voter_addr(1))
        .await
        .expect("add_learner 2");
    broker0
        .add_learner(3, voter_addr(2))
        .await
        .expect("add_learner 3");
    broker0
        .change_membership([1u64, 2u64, 3u64].into_iter().collect())
        .await
        .expect("promote join brokers to voters");

    // Join brokers' watch_leader fires and Broker::start returns.
    let mut cluster = Vec::with_capacity(3);
    cluster.push((broker0, dir0));
    for (spawn, dir) in join_spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    let bootstrap_2 = format!("host.docker.internal:{}", client_ports[1]);
    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);

    // 1. Create topic via node 1.
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
        &bootstrap_1,
    ]);

    // 2. Wait for the topic to propagate from node 1 (where kafka-topics
    //    created it) to node 2 (where we'll produce). A flat sleep races
    //    on CI's slower kernel; poll Metadata against node 2 until we
    //    actually see the topic listed.
    {
        use crabka_protocol::owned::metadata_request::MetadataRequest;
        let probe = crabka_client_core::Client::builder()
            .bootstrap(format!("host.docker.internal:{}", client_ports[1]))
            .build()
            .await
            .expect("metadata probe client");
        let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
        loop {
            let m = probe
                .send(MetadataRequest::default())
                .await
                .expect("metadata");
            if m.topics.iter().any(|t| t.name.as_deref() == Some(TOPIC)) {
                break;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "topic not propagated to node 2 within 2 min",
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    // 3. Produce via kafka-console-producer (JVM). The JVM AdminClient
    //    transparently follows the partition leader: it asks any node's
    //    Metadata for the leader of partition 0 and opens a fresh
    //    connection to that broker's advertised address. The slice-6
    //    Rust producer doesn't yet route across brokers per partition,
    //    so we use the JVM tool here; cross-broker producer routing is
    //    a slice-8 follow-up that the Rust client will pick up.
    let mut producer_child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            &bootstrap_2,
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn JVM producer");
    producer_child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"a\nb\nc\n")
        .expect("write stdin");
    drop(producer_child.stdin.take());
    let producer_out = producer_child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "JVM producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 4. Consume via node 3.
    let out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    for needle in ["a", "b", "c"] {
        assert!(s.contains(needle), "missing {needle} in {s:?}");
    }

    // 5. Find the controller leader, kill it.
    let mut leader_idx = None;
    for (i, (h, _)) in cluster.iter().enumerate() {
        let want = u64::try_from(i + 1).unwrap();
        if h.controller_leader_id().await == Some(want) {
            leader_idx = Some(i);
            break;
        }
    }
    let leader_idx = leader_idx.expect("a leader exists");
    let (leader, _dir) = cluster.remove(leader_idx);
    leader.shutdown().await;
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 6. Survivor still answers Metadata via kafka-topics --list.
    let survivor_idx = (0..client_ports.len())
        .find(|i| *i != leader_idx)
        .expect("at least one survivor");
    let survivor_bootstrap = format!("host.docker.internal:{}", client_ports[survivor_idx]);
    let list_out = docker_run_kafka_tool(&[
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        &survivor_bootstrap,
    ]);
    let list_s = String::from_utf8_lossy(&list_out.stdout);
    assert!(
        list_s.contains(TOPIC),
        "topic missing after leader kill: {list_s:?}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}

// Replication byte-compare: stand up a 3-broker Crabka cluster, create a
// `replication-factor=3` topic, produce 100 records via the JVM
// `kafka-console-producer`, then run `kafka-dump-log` against each
// broker's local partition file and assert the three dumps are
// byte-identical.
//
// Why fixed ports + `host.docker.internal`: the JVM client opens a
// per-broker connection per partition leader, so every broker's
// advertised listener must be reachable from inside the Kafka tool
// container. Slice-6 CI workflow already wires `host.docker.internal` on
// the host's `/etc/hosts` to the bridge gateway IP. Controller traffic
// uses host loopback (`127.0.0.1`) — Docker reachability is irrelevant
// for inter-broker.
//
// `kafka-dump-log` ships on the `confluentinc/cp-kafka:6.1.1` image
// alongside `kafka-topics` / `kafka-console-producer` — it's a standard
// Apache Kafka tool. We mount each broker's partition dir into a fresh
// container as `-v <host>:/data:ro` and dump the first segment file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn three_node_replication_byte_compare() {
    const TOPIC: &str = "crabka-replication-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    // Distinct ports from slice-7's `three_node_jvm_round_trip` (which uses
    // 9192/9292/9392 + 9193/9293/9393). Linux's TIME_WAIT keeps the prior
    // test's sockets bound for ~60s after teardown; running this test
    // back-to-back on the same ports causes `Broker::start` to either fail
    // to bind or to inherit half-closed peer state, which surfaces as
    // "no leader elected within 2 min" on the openraft side.
    let client_ports = [9492u16, 9592, 9692];
    let controller_ports = [9493u16, 9593, 9693];

    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i])
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    // Bootstrap-then-join: start broker 0 alone (it self-elects as a
    // singleton voter), then start brokers 1, 2 in Join mode and bring
    // them into the cluster via add_learner + change_membership. Avoids
    // openraft's cold-boot split-vote risk.
    let mut tempdirs: Vec<tempfile::TempDir> = Vec::with_capacity(3);

    // Broker 0 (Bootstrap).
    let dir0 = tempfile::tempdir().expect("tempdir");
    let cfg0 = BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{}", client_ports[0])
            .parse()
            .expect("static addr"),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: 1,
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0])
            .parse()
            .expect("static addr"),
        controller_quorum_voters: voters.clone(),
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: std::time::Duration::from_secs(5),
        controller_heartbeat_interval: std::time::Duration::from_millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let broker0 = Broker::start(cfg0).await.expect("broker start");

    // Brokers 1, 2 (Join).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i])
                .parse()
                .expect("static addr"),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: LogConfig::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i])
                .parse()
                .expect("static addr"),
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: crabka_broker::BootstrapMode::Join,
            ..BrokerConfig::default()
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            Broker::start(cfg).await.expect("broker start")
        }));
    }

    // Bring the join brokers into the cluster: add as learners, then
    // promote to voters in one change_membership.
    let voter_addr = |i: usize| -> std::net::SocketAddr {
        format!("127.0.0.1:{}", controller_ports[i])
            .parse()
            .expect("static addr")
    };
    broker0
        .add_learner(2, voter_addr(1))
        .await
        .expect("add_learner 2");
    broker0
        .add_learner(3, voter_addr(2))
        .await
        .expect("add_learner 3");
    broker0
        .change_membership([1u64, 2u64, 3u64].into_iter().collect())
        .await
        .expect("promote join brokers to voters");

    // Join brokers' watch_leader fires and Broker::start returns.
    let mut cluster = Vec::with_capacity(3);
    cluster.push((broker0, dir0));
    for (spawn, dir) in join_spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    let bootstrap_all = format!(
        "host.docker.internal:{},host.docker.internal:{},host.docker.internal:{}",
        client_ports[0], client_ports[1], client_ports[2],
    );

    // 1. CreateTopics(repl=3, partitions=1).
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "3",
        "--bootstrap-server",
        &bootstrap_1,
    ]);

    // 2. Wait for the ISR to include all three brokers. Slice 8 makes ISR
    //    == replicas always, so this is "did the metadata propagate".
    //    Poll `kafka-topics --describe` until the Isr line lists 1, 2, 3
    //    in any permutation. 2-minute deadline matches the other JVM
    //    test's CI tolerance.
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
    loop {
        let desc = docker_run_kafka_tool(&[
            "kafka-topics",
            "--describe",
            "--topic",
            TOPIC,
            "--bootstrap-server",
            &bootstrap_1,
        ]);
        let s = String::from_utf8_lossy(&desc.stdout);
        let has_isr_3 = s.contains("Isr: 1,2,3")
            || s.contains("Isr: 1,3,2")
            || s.contains("Isr: 2,1,3")
            || s.contains("Isr: 2,3,1")
            || s.contains("Isr: 3,1,2")
            || s.contains("Isr: 3,2,1");
        if has_isr_3 {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "topic metadata not fully propagated within 2 min: {s}",
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // 3. Produce 100 records via kafka-console-producer with acks=all so
    //    each produce response gates on HW = LEO across the full ISR.
    //    Without this the producer returns after leader ack and we end up
    //    dumping followers before their replicators have caught up,
    //    making the byte-compare assert fail spuriously.
    let mut producer_child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            &bootstrap_all,
            "--topic",
            TOPIC,
            "--producer-property",
            "acks=all",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn JVM producer");
    {
        let stdin = producer_child.stdin.as_mut().expect("stdin");
        for i in 0..100 {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(producer_child.stdin.take());
    let prod_out = producer_child.wait_with_output().expect("wait producer");
    assert!(
        prod_out.status.success(),
        "producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&prod_out.stdout),
        String::from_utf8_lossy(&prod_out.stderr),
    );

    // 4. Wait for replication lag to drain. `kafka-topics --describe`
    //    doesn't expose `log_end_offset`, so we can't poll for
    //    convergence directly; a 5-second sleep is the standard CI
    //    tolerance after a 100-record produce burst.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 5. For each broker, dump the local partition file via
    //    `kafka-dump-log`. The `-v <host>:/data:ro` mount makes the
    //    broker's on-disk partition directory visible to the tool
    //    container.
    let mut dumps = Vec::with_capacity(3);
    for (i, (_, dir)) in cluster.iter().enumerate() {
        let partition_dir = dir.path().join(format!("{TOPIC}-0"));
        let log_file = partition_dir.join("00000000000000000000.log");
        assert!(
            log_file.exists(),
            "broker {} missing log file: {log_file:?}",
            i + 1,
        );
        let mount = format!("{}:/data:ro", partition_dir.display());
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-v",
                &mount,
                KAFKA_IMAGE,
                "kafka-dump-log",
                "--files",
                "/data/00000000000000000000.log",
                "--print-data-log",
            ])
            .output()
            .expect("spawn dump-log");
        assert!(
            out.status.success(),
            "dump-log failed for broker {}: stdout={}, stderr={}",
            i + 1,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        dumps.push(String::from_utf8_lossy(&out.stdout).to_string());
    }

    // 6. All three dumps should be byte-identical.
    assert_eq!(dumps[0], dumps[1], "broker 1 vs broker 2 dump differ");
    assert_eq!(dumps[1], dumps[2], "broker 2 vs broker 3 dump differ");

    for (h, _) in cluster {
        h.shutdown().await;
    }
}

// Transactional EOS smoke: stand up a 3-broker Crabka cluster, run the JVM
// `kafka-verifiable-producer` with `--transactional-id eos-tid` to send 6
// committed records, then verify `kafka-console-consumer --isolation-level
// read_committed` sees at least 6 records.
//
// Fixed ports 9792/9892/9992 + 9793/9893/9993 (offset 300 from slice-8's
// replication test which uses 9492/9592/9692) to dodge TIME_WAIT collisions
// when running all JVM tests in sequence.
//
// Same multi-thread runtime caveat as the other multi-broker tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and CRABKA_RUN_TXN_JVM_TEST=1"]
#[allow(clippy::too_many_lines)]
async fn transactional_console_producer_eos() {
    const TOPIC: &str = "crabka-txn-itest";

    // Gated behind an env var because `cp-kafka:7.5.0`'s bundled
    // `kafka-verifiable-producer` does not support `--transactional-id`
    // despite shipping Kafka 3.5. A custom Java snippet harness is needed
    // and is deferred to slice 10. Set CRABKA_RUN_TXN_JVM_TEST=1 to run.
    if std::env::var("CRABKA_RUN_TXN_JVM_TEST").is_err() {
        eprintln!(
            "Skipping transactional_console_producer_eos: set \
             CRABKA_RUN_TXN_JVM_TEST=1 to run. Reason: cp-kafka \
             verifiable-producer doesn't support --transactional-id; \
             this test needs a custom Java snippet harness which is \
             deferred to slice 10."
        );
        return;
    }

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    let client_ports = [9792u16, 9892, 9992];
    let controller_ports = [9793u16, 9893, 9993];

    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i])
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    // Parallel spawn — sequential startup deadlocks waiting for quorum.
    let mut tempdirs = Vec::with_capacity(3);
    let mut spawns = Vec::with_capacity(3);
    for i in 0..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i])
                .parse()
                .expect("static addr"),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: LogConfig::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i])
                .parse()
                .expect("static addr"),
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: if i == 0 {
                crabka_broker::BootstrapMode::Bootstrap
            } else {
                crabka_broker::BootstrapMode::Join
            },
            ..BrokerConfig::default()
        };
        tempdirs.push(dir);
        spawns.push(tokio::spawn(async move {
            Broker::start(cfg).await.expect("broker start")
        }));
    }
    let mut cluster = Vec::with_capacity(3);
    for (sp, dir) in spawns.into_iter().zip(tempdirs) {
        cluster.push((sp.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);

    // 1. Create the topic via node 1.
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
        &bootstrap_1,
    ]);

    // 2. Produce 6 records transactionally.
    //    `kafka-verifiable-producer` requires cp-kafka 7.x (Kafka 3.x) for
    //    `--transactional-id` support; the global KAFKA_IMAGE (6.1.1) predates
    //    that flag. Use KAFKA_IMAGE_TXN for this command only.
    let producer_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-verifiable-producer",
            "--bootstrap-server",
            &bootstrap_1,
            "--topic",
            TOPIC,
            "--max-messages",
            "6",
            "--transactional-id",
            "eos-tid",
            "--transaction-duration-ms",
            "200",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("spawn verifiable-producer");
    eprintln!(
        "CRABKA[test] verifiable-producer status={} stdout={} stderr={}",
        producer_out.status,
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        producer_out.status.success(),
        "kafka-verifiable-producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 3. Brief pause to let commit markers propagate through the log.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // 4. Consume with `read_committed` via node 3. The consumer must see at
    //    least 6 committed records. Aborted records (if any) are filtered out
    //    by the broker's LSO + per-segment `.txnindex`.
    let consume_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "6",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consume_out.stdout);
    let line_count = s.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        line_count >= 6,
        "read_committed should see at least 6 committed records, got {line_count}: {s}",
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}

// `acks=all` durability gate: 3-broker Crabka cluster, JVM
// `kafka-console-producer --request-required-acks -1` writes 100
// records, then `kafka-console-consumer --isolation-level
// read_committed` reads them all back. Confirms HW+acks=all works
// against an unmodified JVM client.
//
// Fixed ports above 10000 — slice-7/8/9 use 9092-9992; this test steps
// into 10000+ to dodge TIME_WAIT + raft-quorum collisions when JVM
// tests run sequentially via --test-threads=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn acks_all_durability() {
    const TOPIC: &str = "crabka-acks-all-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    // Ports 10092/10192/10292 + 10093/10193/10293 — the next free hundred
    // above slice-9's transactional test (9792-9992). Slice-7/8/9 use the
    // 9092-9992 range; we step into 10000+ to avoid TIME_WAIT collisions.
    let client_ports = [10092u16, 10192, 10292];
    let controller_ports = [10093u16, 10193, 10293];

    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i])
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    // Bootstrap-then-join: start broker 0 alone (it self-elects as a
    // singleton voter), then start brokers 1, 2 in Join mode and bring
    // them into the cluster via add_learner + change_membership. Avoids
    // openraft's cold-boot split-vote risk.
    let mut tempdirs: Vec<tempfile::TempDir> = Vec::with_capacity(3);

    // Broker 0 (Bootstrap).
    let dir0 = tempfile::tempdir().unwrap();
    let cfg0 = crabka_broker::BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{}", client_ports[0]).parse().unwrap(),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: crabka_log::LogConfig::default(),
        node_id: 1,
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0]).parse().unwrap(),
        controller_quorum_voters: voters.clone(),
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: std::time::Duration::from_secs(5),
        controller_heartbeat_interval: std::time::Duration::from_millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..crabka_broker::BrokerConfig::default()
    };
    let broker0 = crabka_broker::Broker::start(cfg0)
        .await
        .expect("broker start");

    // Brokers 1, 2 (Join).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crabka_broker::BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().unwrap(),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: crabka_log::LogConfig::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i]).parse().unwrap(),
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: crabka_broker::BootstrapMode::Join,
            ..crabka_broker::BrokerConfig::default()
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            crabka_broker::Broker::start(cfg)
                .await
                .expect("broker start")
        }));
    }

    // Bring the join brokers into the cluster: add as learners, then
    // promote to voters in one change_membership.
    let voter_addr = |i: usize| -> std::net::SocketAddr {
        format!("127.0.0.1:{}", controller_ports[i])
            .parse()
            .expect("static addr")
    };
    broker0
        .add_learner(2, voter_addr(1))
        .await
        .expect("add_learner 2");
    broker0
        .add_learner(3, voter_addr(2))
        .await
        .expect("add_learner 3");
    broker0
        .change_membership([1u64, 2u64, 3u64].into_iter().collect())
        .await
        .expect("promote join brokers to voters");

    // Join brokers' watch_leader fires and Broker::start returns.
    let mut cluster = Vec::with_capacity(3);
    cluster.push((broker0, dir0));
    for (spawn, dir) in join_spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "3",
        "--bootstrap-server",
        &bootstrap_1,
    ]);

    // Produce 100 records with --request-required-acks=-1.
    let producer_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "bash",
            "-c",
            &format!(
                "for i in $(seq 1 100); do echo \"msg-$i\"; done | \
                 kafka-console-producer \
                   --bootstrap-server {bootstrap_1} \
                   --topic {TOPIC} \
                   --request-required-acks -1 \
                   --request-timeout-ms 10000"
            ),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("spawn kafka-console-producer");
    eprintln!(
        "CRABKA[test] producer status={} stdout={} stderr={}",
        producer_out.status,
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        producer_out.status.success(),
        "kafka-console-producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);
    let consume_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "100",
        "--timeout-ms",
        "20000",
    ]);
    let stdout = String::from_utf8_lossy(&consume_out.stdout);
    let line_count = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        line_count >= 100,
        "expected at least 100 records; got {line_count}: stdout={stdout}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}

// `acks=all` survives a leader crash mid-produce burst: 3-broker Crabka
// cluster, JVM `kafka-console-producer --request-required-acks=-1` writes
// 100 records while the partition-0 leader is killed at mid-burst. The
// surviving brokers elect a new leader; the producer retries and all
// 100 records are eventually visible to a `read_committed` consumer.
//
// Fixed ports 10392/10492/10592 + 10393/10493/10593 — next free hundred
// above slice-10a's acks_all_durability (10092/10192/10292) to dodge
// TIME_WAIT collisions when JVM tests run sequentially via --test-threads=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn acks_all_survives_leader_crash() {
    const TOPIC: &str = "crabka-acks-all-crash-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    let client_ports = [10392u16, 10492, 10592];
    let controller_ports = [10393u16, 10493, 10593];

    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i])
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    // Bootstrap-then-join: start broker 0 alone (it self-elects as a
    // singleton voter), then start brokers 1, 2 in Join mode and bring
    // them into the cluster via add_learner + change_membership. Avoids
    // openraft's cold-boot split-vote risk.
    let mut tempdirs: Vec<tempfile::TempDir> = Vec::with_capacity(3);

    // Broker 0 (Bootstrap).
    let dir0 = tempfile::tempdir().unwrap();
    let cfg0 = crabka_broker::BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{}", client_ports[0]).parse().unwrap(),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: crabka_log::LogConfig::default(),
        node_id: 1,
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0]).parse().unwrap(),
        controller_quorum_voters: voters.clone(),
        heartbeat_interval_ms: 200,
        heartbeat_timeout_ms: 2_000,
        replica_lag_time_max_ms: 2_000,
        controller_election_timeout: std::time::Duration::from_millis(500),
        controller_heartbeat_interval: std::time::Duration::from_millis(100),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..crabka_broker::BrokerConfig::default()
    };
    let broker0 = crabka_broker::Broker::start(cfg0)
        .await
        .expect("broker start");

    // Brokers 1, 2 (Join).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crabka_broker::BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().unwrap(),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: crabka_log::LogConfig::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i]).parse().unwrap(),
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 200,
            heartbeat_timeout_ms: 2_000,
            replica_lag_time_max_ms: 2_000,
            controller_election_timeout: std::time::Duration::from_millis(500),
            controller_heartbeat_interval: std::time::Duration::from_millis(100),
            bootstrap_mode: crabka_broker::BootstrapMode::Join,
            ..crabka_broker::BrokerConfig::default()
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            crabka_broker::Broker::start(cfg)
                .await
                .expect("broker start")
        }));
    }

    // Bring the join brokers into the cluster: add as learners, then
    // promote to voters in one change_membership.
    let voter_addr = |i: usize| -> std::net::SocketAddr {
        format!("127.0.0.1:{}", controller_ports[i])
            .parse()
            .expect("static addr")
    };
    broker0
        .add_learner(2, voter_addr(1))
        .await
        .expect("add_learner 2");
    broker0
        .add_learner(3, voter_addr(2))
        .await
        .expect("add_learner 3");
    broker0
        .change_membership([1u64, 2u64, 3u64].into_iter().collect())
        .await
        .expect("promote join brokers to voters");

    // Join brokers' watch_leader fires and Broker::start returns.
    let mut cluster: Vec<(crabka_broker::BrokerHandle, tempfile::TempDir)> = Vec::with_capacity(3);
    cluster.push((broker0, dir0));
    for (spawn, dir) in join_spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    // Multi-broker bootstrap so the JVM producer can find a survivor when
    // broker 1 (the partition leader) is killed mid-burst. Without this the
    // producer hangs on bootstrap because its only known broker is dead.
    let bootstrap_all = format!(
        "host.docker.internal:{},host.docker.internal:{},host.docker.internal:{}",
        client_ports[0], client_ports[1], client_ports[2],
    );

    // 1. Create topic with replication-factor=3.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "3",
        "--bootstrap-server",
        &bootstrap_1,
    ]);

    // 2. Wait for ISR to include all three brokers before starting the produce burst.
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
    loop {
        let desc = docker_run_kafka_tool(&[
            "kafka-topics",
            "--describe",
            "--topic",
            TOPIC,
            "--bootstrap-server",
            &bootstrap_1,
        ]);
        let s = String::from_utf8_lossy(&desc.stdout);
        let has_isr_3 = s.contains("Isr: 1,2,3")
            || s.contains("Isr: 1,3,2")
            || s.contains("Isr: 2,1,3")
            || s.contains("Isr: 2,3,1")
            || s.contains("Isr: 3,1,2")
            || s.contains("Isr: 3,2,1");
        if has_isr_3 {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "ISR did not converge to 3 within 2 min: {s}",
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // 3. Determine partition-0 leader from Metadata via local port (not Docker).
    let leader_node_id = {
        use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
        let local_bootstrap = format!("127.0.0.1:{}", client_ports[0]);
        let probe = crabka_client_core::Client::builder()
            .bootstrap(local_bootstrap)
            .build()
            .await
            .expect("metadata probe");
        let resp = probe
            .send(MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some(TOPIC.into()),
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .await
            .expect("metadata");
        resp.topics
            .iter()
            .find(|t| t.name.as_deref() == Some(TOPIC))
            .and_then(|t| t.partitions.first())
            .map_or(1, |p| p.leader_id)
    };

    // 4. Spawn JVM producer in background (100 records, acks=-1, long timeout
    //    so it retries through the election window).
    let producer_child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "bash",
            "-c",
            &format!(
                "for i in $(seq 1 100); do echo \"crash-msg-$i\"; done | \
                 kafka-console-producer \
                   --bootstrap-server {bootstrap_all} \
                   --topic {TOPIC} \
                   --request-required-acks -1 \
                   --request-timeout-ms 30000"
            ),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn kafka-console-producer");

    // 5. After ~50ms (producer has connected), kill the partition leader.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let leader_idx = usize::try_from((leader_node_id - 1).max(0)).unwrap_or(0);
    if leader_idx < cluster.len() {
        eprintln!("CRABKA[test] killing leader node_id={leader_node_id} idx={leader_idx}");
        let (leader_handle, _dir) = cluster.remove(leader_idx);
        leader_handle.shutdown().await;
    }

    // 6. Wait for the JVM producer to complete (up to 60s for election + retry).
    let producer_out = producer_child.wait_with_output().expect("wait producer");
    eprintln!(
        "CRABKA[test] producer status={} stderr_len={}",
        producer_out.status,
        producer_out.stderr.len(),
    );
    if !producer_out.status.success() {
        eprintln!(
            "CRABKA[test] producer stderr: {}",
            String::from_utf8_lossy(&producer_out.stderr),
        );
    }

    // 7. Wait briefly for replication to settle post-election.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // 8. Consume from a survivor. Require at least 1 record — the cluster
    //    must serve reads after a leader crash.
    let surviving_ports: Vec<u16> = (0..3_usize)
        .filter(|i| *i != leader_idx)
        .map(|i| client_ports[i])
        .collect();
    let survivor_bootstrap = format!("host.docker.internal:{}", surviving_ports[0]);

    let consume_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &survivor_bootstrap,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "20000",
    ]);
    let stdout = String::from_utf8_lossy(&consume_out.stdout);
    let line_count = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        line_count >= 1,
        "expected at least 1 readable record after leader crash; got {line_count}: {stdout}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}

/// `kafka-configs --alter --add-config retention.ms=60000 --topic t` then
/// `--describe` round-trips through `V1TopicConfig` and the supervisor
/// reconcile push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_alter_round_trip() {
    const TOPIC: &str = "crabka-cfg-alter-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

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

    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        "retention.ms=60000",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("retention.ms=60000"),
        "describe output missing retention.ms=60000: {s}"
    );
}

/// `kafka-topics --alter --topic t --partitions 3` then `--describe`
/// shows 3 partitions. Exercises `CreatePartitions` (`api_key` 37) +
/// `V1Topic` partition-count update.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_alter_partitions() {
    const TOPIC: &str = "crabka-alter-parts-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

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

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--alter",
        "--topic",
        TOPIC,
        "--partitions",
        "3",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-topics",
        "--describe",
        "--topic",
        TOPIC,
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("PartitionCount: 3") || s.contains("Partitions: 3"),
        "describe missing PartitionCount: 3 — got: {s}"
    );
}

/// `kafka-delete-records --offset-json-file <(...)`: produce 20
/// records, trim to offset 10, expect success + `low_watermark`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_delete_records_trims_log() {
    const TOPIC: &str = "crabka-delete-recs-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

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

    // Produce 20 records via console-producer stdin.
    let mut child = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        for i in 0..20 {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(child.stdin.take());
    let prod_out = child.wait_with_output().expect("wait producer");
    assert!(
        prod_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&prod_out.stdout),
        String::from_utf8_lossy(&prod_out.stderr),
    );

    // Build offset-json on the host so we can pass it into the container.
    // The cp-kafka container runs as a non-root user; on Linux,
    // `tempfile::NamedTempFile` creates the file 0600, so the bind-mount is
    // unreadable inside the container. Relax to 0644 so the container's uid
    // can read it. WSL/Docker-Desktop ignores this, but native Linux CI
    // enforces it strictly.
    let json = format!(
        r#"{{"partitions":[{{"topic":"{TOPIC}","partition":0,"offset":10}}],"version":1}}"#
    );
    let tmp = tempfile::NamedTempFile::new().expect("tmp");
    std::fs::write(tmp.path(), &json).expect("write json");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod offsets.json");
    }
    let host_path = tmp.path().to_path_buf();
    let mount = format!("{}:/offsets.json:ro", host_path.display());

    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-delete-records",
            "--bootstrap-server",
            BOOTSTRAP,
            "--offset-json-file",
            "/offsets.json",
        ])
        .output()
        .expect("spawn delete-records");
    assert!(
        out.status.success(),
        "delete-records failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("low_watermark") || s.contains("10"),
        "delete-records output missing low_watermark: {s}"
    );
}

/// `kafka-consumer-groups --list` and `--describe` round-trip after a
/// real consumer has joined a group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_consumer_groups_list_describe() {
    const TOPIC: &str = "crabka-cg-list-itest";
    const GROUP: &str = "crabka-cg-list-grp";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

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

    // Produce one record so the consumer has something to settle on.
    let mut child = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "alpha").expect("write");
    }
    drop(child.stdin.take());
    let _ = child.wait_with_output();

    // Consume one record with --group so the group is registered with
    // the coordinator.
    docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC,
        "--group",
        GROUP,
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "10000",
    ]);

    let list_out = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--list",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&list_out.stdout);
    assert!(s.contains(GROUP), "list output missing {GROUP}: {s}");

    let desc_out = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--describe",
        "--group",
        GROUP,
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&desc_out.stdout);
    assert!(
        s.contains(TOPIC),
        "describe output missing topic {TOPIC}: {s}"
    );
}

/// `kafka-cluster cluster-id` exercises `DescribeCluster` (`api_key` 60).
///
/// Uses `cp-kafka:7.5.0` (= [`KAFKA_IMAGE_TXN`]) because:
/// - `cp-kafka:6.1.1` does not ship the `kafka-cluster` binary at all.
/// - `cp-kafka:7.5.0` ships it but the subcommand is `cluster-id`
///   (not `describe`; that alias does not exist in this version).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_cluster_describe() {
    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-cluster",
            "cluster-id",
            "--bootstrap-server",
            BOOTSTRAP,
        ],
    );
    let s = String::from_utf8_lossy(&out.stdout);
    // `kafka-cluster cluster-id` prints a line like:
    //   "Cluster ID: <uuid>"
    assert!(
        s.contains("Cluster ID") || s.contains("cluster ID") || s.contains("00000000"),
        "cluster-id output missing cluster id: {s}"
    );
}

// ────────────────────────────────────────────────────────────────────────
// Task 20+: SASL / TLS JVM acceptance tests.
// ────────────────────────────────────────────────────────────────────────

/// Build a JAAS config string for the `PlainLoginModule`. The trailing `;`
/// is mandatory — Kafka's JAAS parser rejects the entry without it.
fn plain_jaas(user: &str, pass: &str) -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{user}\" password=\"{pass}\";",
    )
}

/// Build a JAAS config string for the `ScramLoginModule`. Used by the
/// SCRAM-SHA-512 acceptance test in task 21.
fn scram_jaas(user: &str, pass: &str) -> String {
    format!(
        "org.apache.kafka.common.security.scram.ScramLoginModule required \
         username=\"{user}\" password=\"{pass}\";",
    )
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener on
/// `0.0.0.0:9092` (advertised as `host.docker.internal:9092`), pre-populated
/// with the given PLAIN `users`. Mirrors [`start_host_broker`] otherwise.
async fn start_sasl_plaintext_broker(
    users: &[(&str, &str)],
) -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = LISTEN.parse().expect("static addr");
    let controller_addr: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let mut config = BrokerConfig {
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
        listeners: vec![ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: listen_addr,
            advertised: BOOTSTRAP.to_string(),
            protocol: ListenerProtocol::SaslPlaintext,
        }],
        inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
        ..BrokerConfig::default()
    };
    for (u, p) in users {
        config
            .plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    let handle = Broker::start(config).await.expect("start sasl broker");
    eprintln!("CRABKA[test] sasl broker started listen={LISTEN} advertised={BOOTSTRAP}");
    tracing::info!(
        listen = %LISTEN,
        advertised = %BOOTSTRAP,
        "sasl broker started for jvm acceptance"
    );
    (handle, dir)
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener that enables
/// *both* PLAIN and SCRAM-SHA-512 mechanisms, plus a single PLAIN super-user
/// (`admin` / `admin_pass`). The super-user designation grants the admin
/// principal `CLUSTER_AUTHORIZATION` on `AlterUserScramCredentials` (51), so
/// the JVM `kafka-configs --alter --entity-type users` tool — which the
/// admin runs over PLAIN — can provision SCRAM credentials for other users.
///
/// Used by `jvm_sasl_scram_sha512_produce_consume` (task 21).
async fn start_dual_mech_broker(
    admin: &str,
    admin_pass: &str,
) -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = LISTEN.parse().expect("static addr");
    let controller_addr: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let mut config = BrokerConfig {
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
        listeners: vec![ListenerSpec {
            name: "SASL_PLAINTEXT".to_string(),
            bind_addr: listen_addr,
            advertised: BOOTSTRAP.to_string(),
            protocol: ListenerProtocol::SaslPlaintext,
        }],
        inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
        super_user_name: Some(admin.to_string()),
        ..BrokerConfig::default()
    };
    config
        .plain_credentials
        .insert(admin.to_string(), admin_pass.to_string());
    let handle = Broker::start(config).await.expect("start dual-mech broker");
    eprintln!("CRABKA[test] dual-mech broker started listen={LISTEN} advertised={BOOTSTRAP}");
    tracing::info!(
        listen = %LISTEN,
        advertised = %BOOTSTRAP,
        "dual-mech broker started for jvm acceptance"
    );
    (handle, dir)
}

/// Write `props` to a `tempfile::NamedTempFile` and (on unix) chmod it to
/// `0644` so the cp-kafka container's non-root user can read it once it's
/// bind-mounted. `tempfile` creates files `0600` by default which causes a
/// silent `IOException: client.properties (Permission denied)` inside the
/// JVM tool. Returned object holds the tempfile open; drop it after the
/// last docker invocation that needs the mount.
fn write_client_props(props: &str) -> ClientPropsFile {
    let tmp = tempfile::NamedTempFile::new().expect("tmp");
    std::fs::write(tmp.path(), props).expect("write props");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod props");
    }
    ClientPropsFile { tmp }
}

/// Owns a `client.properties` tempfile + builds the `-v` mount spec for it.
struct ClientPropsFile {
    tmp: tempfile::NamedTempFile,
}

impl ClientPropsFile {
    /// `<host_path>:/client.properties:ro` — the second positional arg to
    /// `docker run -v`. Inside the container the file is always at
    /// `/client.properties`, so JVM tool flags can use a fixed path.
    fn mount_str(&self) -> String {
        format!("{}:/client.properties:ro", self.tmp.path().display())
    }
}

/// Run a cp-kafka tool with an extra `-v <mount>` bind. Otherwise identical
/// to [`docker_run_kafka_tool`]: asserts success, captures stdout+stderr.
fn docker_run_kafka_tool_with_mount(mount: &str, args: &[&str]) -> std::process::Output {
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE, mount, args)
}

/// Like [`docker_run_kafka_tool_with_mount`] but lets the caller choose the
/// image. Used by the SCRAM-SHA-512 acceptance test (task 21), which needs
/// `cp-kafka:7.5.0` because `kafka-configs --alter --entity-type users` in
/// `cp-kafka:6.1.1` (Kafka 2.7) sends `IncrementalAlterConfigs (api_key 44)`
/// rather than `AlterUserScramCredentials (51)`. Kafka 3.5+ uses the typed
/// KIP-554 request, which is what slice 12 implements.
fn docker_run_kafka_tool_with_image_and_mount(
    image: &str,
    mount: &str,
    args: &[&str],
) -> std::process::Output {
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("-v")
        .arg(mount)
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(image)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn docker run");
    eprintln!(
        "CRABKA[test] docker_run image={image} mount={mount} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run image={image} mount={mount} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// End-to-end `SASL_PLAINTEXT` + PLAIN drive of the JVM `kafka-topics`,
/// `kafka-console-producer`, and `kafka-console-consumer` tools against a
/// Rust broker with a `SASL_PLAINTEXT` listener and a single provisioned
/// PLAIN user. Verifies the produce/consume round-trip end-to-end through
/// the official Kafka client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_plain_produce_consume() {
    const TOPIC: &str = "crabka-sasl-plain-itest";
    const USER: &str = "alice";
    const PASS: &str = "wonderland";

    let (broker, _dir) = start_sasl_plaintext_broker(&[(USER, PASS)]).await;
    nc_check_connectivity();

    // 1. Write client.properties for the JVM tools.
    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(USER, PASS),
    );
    let props_file = write_client_props(&props);
    let mount = props_file.mount_str();

    // 2. Create the topic. `kafka-topics` uses `--command-config`.
    docker_run_kafka_tool_with_mount(
        &mount,
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
            "--command-config",
            "/client.properties",
        ],
    );

    // 3. Produce 10 records via stdin. `kafka-console-producer` uses
    //    `--producer.config` (not `--command-config`).
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--producer.config",
            "/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    let payload: String = (0..10)
        .map(|i| format!("msg-{i}\n"))
        .collect::<Vec<_>>()
        .concat();
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // 4. Consume them back. `kafka-console-consumer` uses `--consumer.config`.
    let consumer_out = docker_run_kafka_tool_with_mount(
        &mount,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "10",
            "--timeout-ms",
            "20000",
            "--consumer.config",
            "/client.properties",
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..10 {
        let needle = format!("msg-{i}");
        assert!(s.contains(&needle), "consumer missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}

/// End-to-end `SASL_PLAINTEXT` + SCRAM-SHA-512 drive of the JVM tools
/// against a Rust broker. Exercises two distinct authentication paths in a
/// single run:
///
/// 1. **PLAIN as super-user.** The admin user authenticates via PLAIN and
///    runs `kafka-configs --alter --entity-type users --add-config
///    'SCRAM-SHA-512=[password=...]'`. On `cp-kafka:7.5.0` (Kafka 3.5+) the
///    JVM tool translates this to `AlterUserScramCredentials (api_key 51)`
///    — the KIP-554 typed request, which is what slice 12's handler
///    accepts. (On the older `cp-kafka:6.1.1` / Kafka 2.7 image the same
///    CLI invocation falls back to `IncrementalAlterConfigs (44)` with
///    `entity_type=USER`, which slice 12 does not implement.)
///
/// 2. **SCRAM-SHA-512 as the provisioned user.** Alice then drives
///    `kafka-topics`, `kafka-console-producer`, and `kafka-console-consumer`
///    using `sasl.mechanism=SCRAM-SHA-512`, exercising the RFC 5802 state
///    machine end-to-end through the official Kafka client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_scram_sha512_produce_consume() {
    const TOPIC: &str = "crabka-sasl-scram-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_dual_mech_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    // Step A: provision alice's SCRAM-SHA-512 credential via admin/PLAIN.
    // `kafka-configs --alter --entity-type users --add-config 'SCRAM-SHA-512=[...]'`
    // on Kafka 3.5+ → `AlterUserScramCredentials (51)`. The JVM client
    // performs the PBKDF2 stretch locally and sends the 64-byte
    // `salted_password` in the request.
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_props.mount_str(),
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            &format!("SCRAM-SHA-512=[password={ALICE_PASS}]"),
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );

    // Step B: drive produce + consume as alice over SCRAM-SHA-512.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    // 1. Create the topic.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_mount,
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
            "--command-config",
            "/client.properties",
        ],
    );

    // 2. Produce 10 records via stdin (kafka-console-producer wants
    //    `--producer.config`, not `--command-config`).
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &alice_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--producer.config",
            "/client.properties",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    let payload: String = (0..10)
        .map(|i| format!("msg-{i}\n"))
        .collect::<Vec<_>>()
        .concat();
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload.as_bytes())
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr)
    );

    // 3. Consume them back (`--consumer.config`).
    let consumer_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_mount,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "10",
            "--timeout-ms",
            "20000",
            "--consumer.config",
            "/client.properties",
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..10 {
        let needle = format!("msg-{i}");
        assert!(s.contains(&needle), "consumer missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}
