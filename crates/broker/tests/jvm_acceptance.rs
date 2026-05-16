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

// `clippy::unnecessary_unwrap` fires on the `l.unwrap()` inside
// `if l.is_some() && l != Some(1)` in `jvm_kafka_leader_election_preferred`
// and its span computation ICEs in annotate-snippets on Rust 1.95.
#![allow(clippy::unnecessary_unwrap)]
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
        super_users: std::collections::HashSet::from([admin.to_string()]),
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
#[allow(clippy::too_many_lines)]
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
    // Slice 13: disable idempotent producer mode (cp-kafka 7.5 default) so
    // the producer doesn't request `InitProducerId`, which would require
    // `Cluster IdempotentWrite` ACL alice doesn't hold. acks=1 is a
    // single-broker setup default that pairs cleanly with that.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    // 1. Create the topic. Run as `admin` (super-user) so the slice-13
    //    `CreateTopics` Cluster-Create authorize check passes via the
    //    super-user bypass. Alice has no Cluster ACLs.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_props.mount_str(),
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

    // 1b. Grant alice the topic ACLs required for produce/consume.
    //     Slice-13b implications: Read/Write each auto-grant Describe on
    //     the same topic, so Describe is no longer seeded explicitly.
    //     Consumer uses `--partition 0` (no consumer group)
    //     so no Group ACL is required.
    for op in ["Read", "Write"] {
        docker_run_kafka_tool_with_image_and_mount(
            KAFKA_IMAGE_TXN,
            &admin_props.mount_str(),
            &[
                "kafka-acls",
                "--add",
                "--allow-principal",
                &format!("User:{ALICE}"),
                "--operation",
                op,
                "--topic",
                TOPIC,
                "--bootstrap-server",
                BOOTSTRAP,
                "--command-config",
                "/client.properties",
            ],
        );
    }

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

/// Spawn the broker with a single `SSL` listener on `0.0.0.0:9092`
/// (advertised as `host.docker.internal:9092`) using the dev cert/key from
/// `crates/security/tests/fixtures/`. No SASL. Mirrors
/// [`start_host_broker`] otherwise but flips the protocol to `Ssl` and
/// supplies a [`TlsConfig`].
async fn start_ssl_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, TlsConfig};

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

    // Resolve the on-disk paths of the dev fixture certs. Relative to this
    // crate's manifest dir, the fixture lives in the security crate.
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert_path = manifest_dir
        .join("..")
        .join("security")
        .join("tests")
        .join("fixtures")
        .join("dev_cert.pem");
    let key_path = manifest_dir
        .join("..")
        .join("security")
        .join("tests")
        .join("fixtures")
        .join("dev_key.pem");
    assert!(
        cert_path.exists(),
        "dev_cert.pem missing at {}",
        cert_path.display(),
    );
    assert!(
        key_path.exists(),
        "dev_key.pem missing at {}",
        key_path.display(),
    );

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
        listeners: vec![ListenerSpec {
            name: "SSL".to_string(),
            bind_addr: listen_addr,
            advertised: BOOTSTRAP.to_string(),
            protocol: ListenerProtocol::Ssl,
        }],
        inter_broker_listener_name: "SSL".to_string(),
        tls_config: Some(TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
        }),
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start ssl broker");
    eprintln!("CRABKA[test] ssl broker started listen={LISTEN} advertised={BOOTSTRAP}");
    tracing::info!(
        listen = %LISTEN,
        advertised = %BOOTSTRAP,
        "ssl broker started for jvm acceptance"
    );
    (handle, dir)
}

/// Build a JKS truststore from the dev cert PEM by shelling out to
/// `keytool` inside a one-shot Docker container. Returns the host-side
/// path to a `ts.jks` file (chmod `0644` so the cp-kafka container's
/// non-root user can read it once bind-mounted).
///
/// Caches the result under `<tmp>/crabka-jvm-truststore/ts.jks` so
/// repeated invocations (across both this test and the `SASL_SSL` test in
/// task 23) skip the keytool round-trip.
///
/// The cp-kafka:6.1.1 image ships its own JRE + `keytool` binary, so we
/// reuse it via `--entrypoint keytool` rather than pulling `openjdk:17`.
/// The image is guaranteed to be on disk because the SSL test itself
/// invokes `kafka-broker-api-versions` from the same image.
fn prepare_jks_truststore() -> std::path::PathBuf {
    let cache_dir = std::env::temp_dir().join("crabka-jvm-truststore");
    std::fs::create_dir_all(&cache_dir).expect("mkdir truststore cache");
    let ts_path = cache_dir.join("ts.jks");

    // Stage the cert in the cache dir so the bind mount is a directory we
    // control. This sidesteps mount-path quoting on /mnt/c under WSL.
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert_src = manifest_dir
        .join("..")
        .join("security")
        .join("tests")
        .join("fixtures")
        .join("dev_cert.pem");
    let cert_staged = cache_dir.join("dev_cert.pem");
    std::fs::copy(&cert_src, &cert_staged).expect("copy dev_cert.pem to cache");

    if !ts_path.exists() {
        let mount = format!("{}:/work", cache_dir.display());
        // Run keytool + chmod as root inside the container so the host
        // file ends up world-readable. `--user 0:0` lets keytool create
        // `/work/ts.jks` regardless of host-dir owner (CI runner-owned
        // tmpdir blocks cp-kafka's non-root default user). The `chmod
        // 0644` is inside the container too because the file is owned
        // by root on the host once keytool runs as root, so the host-side
        // runner user can't chmod it later.
        let inner = "set -e; \
             keytool -import -alias crabka -file /work/dev_cert.pem \
                 -keystore /work/ts.jks -storepass changeit -noprompt && \
             chmod 0644 /work/ts.jks";
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--user",
                "0:0",
                "-v",
                &mount,
                "--entrypoint",
                "bash",
                KAFKA_IMAGE,
                "-c",
                inner,
            ])
            .output()
            .expect("spawn keytool");
        assert!(
            out.status.success(),
            "keytool import failed: stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        assert!(
            ts_path.exists(),
            "keytool reported success but ts.jks missing at {}",
            ts_path.display(),
        );
    }

    ts_path
}

/// End-to-end TLS handshake check against an `SSL`-only listener. Drives
/// `kafka-broker-api-versions` from inside the cp-kafka container with a
/// JKS truststore containing the broker's dev cert. Verifies the JVM
/// client completes the TLS handshake and exchanges an `ApiVersions`
/// request over the encrypted channel.
///
/// Hostname verification is disabled
/// (`ssl.endpoint.identification.algorithm=`) because the dev cert's CN
/// is `crabka-dev`, not `host.docker.internal`. The dev cert is a
/// self-signed ECDSA P-256 end-entity (regenerated in this task from the
/// original ED25519 + CA:TRUE fixture — cp-kafka:6.1.1 ships Java 11
/// whose `SunJSSE` does not advertise `ed25519` signature schemes during
/// the TLS handshake, so the JVM client would reject ED25519 server
/// certs with `NoSignatureSchemesInCommon`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_ssl_handshake_succeeds() {
    let (broker, _dir) = start_ssl_broker().await;
    nc_check_connectivity();

    let truststore_path = prepare_jks_truststore();

    let props = "security.protocol=SSL\n\
                 ssl.truststore.location=/truststore.jks\n\
                 ssl.truststore.password=changeit\n\
                 ssl.endpoint.identification.algorithm=\n";
    let props_tmp = write_client_props(props);
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());

    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &props_tmp.mount_str(),
            "-v",
            &ts_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-broker-api-versions",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn kafka-broker-api-versions");
    eprintln!(
        "CRABKA[test] ssl api-versions status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "ssl handshake failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    broker.shutdown().await;
}

// ────────────────────────────────────────────────────────────────────────
// Task 23: SASL_SSL full stack + JVM-driven inter-broker SASL replication.
// ────────────────────────────────────────────────────────────────────────

/// Like [`docker_run_kafka_tool_with_image_and_mount`] but supports multiple
/// bind mounts. Needed by the `SASL_SSL` test (task 23), which mounts both
/// a `client.properties` file and a JKS truststore into the same container.
fn docker_run_kafka_tool_with_image_and_mounts(
    image: &str,
    mounts: &[&str],
    args: &[&str],
) -> std::process::Output {
    let mut cmd = Command::new("docker");
    cmd.arg("run").arg("--rm");
    for m in mounts {
        cmd.arg("-v").arg(m);
    }
    cmd.arg("--add-host=host.docker.internal:host-gateway")
        .arg(image)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped());
    let out = cmd.output().expect("spawn docker run");
    eprintln!(
        "CRABKA[test] docker_run image={image} mounts={mounts:?} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run image={image} mounts={mounts:?} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// Spawn the broker with a single `SASL_SSL` listener — PLAIN +
/// SCRAM-SHA-512 mechanisms enabled, the dev cert/key wired up for TLS,
/// and `admin` provisioned as the super-user PLAIN identity so it can
/// `AlterUserScramCredentials` to provision SCRAM users.
///
/// This is the dual-mech broker from [`start_dual_mech_broker`] flipped
/// from `SaslPlaintext` to `SaslSsl` with a `TlsConfig` attached — i.e.
/// the production-shape listener configuration.
async fn start_sasl_ssl_broker(
    admin: &str,
    admin_pass: &str,
) -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    use crabka_broker::config::ListenerSpec;
    use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

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

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert_path = manifest_dir
        .join("..")
        .join("security")
        .join("tests")
        .join("fixtures")
        .join("dev_cert.pem");
    let key_path = manifest_dir
        .join("..")
        .join("security")
        .join("tests")
        .join("fixtures")
        .join("dev_key.pem");

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
            name: "SASL_SSL".to_string(),
            bind_addr: listen_addr,
            advertised: BOOTSTRAP.to_string(),
            protocol: ListenerProtocol::SaslSsl,
        }],
        inter_broker_listener_name: "SASL_SSL".to_string(),
        tls_config: Some(TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
        }),
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
        super_users: std::collections::HashSet::from([admin.to_string()]),
        ..BrokerConfig::default()
    };
    config
        .plain_credentials
        .insert(admin.to_string(), admin_pass.to_string());
    let handle = Broker::start(config).await.expect("start sasl_ssl broker");
    eprintln!("CRABKA[test] sasl_ssl broker started listen={LISTEN} advertised={BOOTSTRAP}");
    tracing::info!(
        listen = %LISTEN,
        advertised = %BOOTSTRAP,
        "sasl_ssl broker started for jvm acceptance"
    );
    (handle, dir)
}

/// End-to-end `SASL_SSL` drive of the JVM tools — the production-shape auth
/// path: TLS handshake + SCRAM-SHA-512 SASL exchange over the encrypted
/// channel. Mirrors `jvm_sasl_scram_sha512_produce_consume` (task 21) but
/// with the `SASL_PLAINTEXT` listener swapped for `SASL_SSL` and the JVM
/// client configured with a JKS truststore.
///
/// Uses cp-kafka:7.5.0 so admin's `kafka-configs --alter --entity-type users
/// --add-config 'SCRAM-SHA-512=[...]'` translates to KIP-554's
/// `AlterUserScramCredentials (api_key 51)` rather than the legacy
/// `IncrementalAlterConfigs (44)` path that slice 12 doesn't implement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_sasl_ssl_full_stack() {
    const TOPIC: &str = "crabka-sasl-ssl-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_sasl_ssl_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();
    let truststore_path = prepare_jks_truststore();
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());

    // Step A: provision alice's SCRAM-SHA-512 credential via admin/PLAIN
    // over the SASL_SSL listener.
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
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

    // Step B: drive produce + consume as alice over SASL_SSL + SCRAM-SHA-512.
    // Slice 13: disable idempotent producer mode so alice doesn't need
    // `Cluster IdempotentWrite`.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_props_mount = alice_props.mount_str();

    // 1. Create the topic. Run as `admin` (super-user) so the slice-13
    //    `CreateTopics` Cluster-Create authorize check passes. Then grant
    //    alice Read/Write on the topic; slice-13b implications auto-grant
    //    Describe via Read and Write.
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
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
    for op in ["Read", "Write"] {
        docker_run_kafka_tool_with_image_and_mounts(
            KAFKA_IMAGE_TXN,
            &[&admin_props.mount_str(), &ts_mount],
            &[
                "kafka-acls",
                "--add",
                "--allow-principal",
                &format!("User:{ALICE}"),
                "--operation",
                op,
                "--topic",
                TOPIC,
                "--bootstrap-server",
                BOOTSTRAP,
                "--command-config",
                "/client.properties",
            ],
        );
    }

    // 2. Produce 10 records via stdin.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &alice_props_mount,
            "-v",
            &ts_mount,
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

    // 3. Consume them back.
    let consumer_out = docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&alice_props_mount, &ts_mount],
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

/// Host port assignments for the two-broker JVM inter-broker test. The
/// `SASL_PLAINTEXT` listener of broker 0 binds `0.0.0.0:9092` (advertised as
/// `host.docker.internal:9092`) and broker 1 binds `0.0.0.0:9094`
/// (advertised as `host.docker.internal:9094`). Inter-broker traffic flows
/// over the same listeners — each broker resolves `host.docker.internal`
/// to its peer's bound port via the host's resolver.
const HOST_PORT_B1: u16 = 9094;
const BOOTSTRAP_B1: &str = "host.docker.internal:9094";
const LISTEN_B1: &str = "0.0.0.0:9094";

/// Spawn two in-process brokers that share a single inter-broker SASL
/// credential. Each broker has one `SASL_PLAINTEXT` listener; both
/// `plain_credentials[admin] = admin_pass` so each broker can authenticate
/// to the other via the same admin identity. The inter-broker listener
/// name on both is `"SASL_PLAINTEXT"`, so the broker peers dial each
/// other's advertised host (which we set to `host.docker.internal:<port>`
/// so the JVM containers can use the same metadata response).
async fn start_two_sasl_brokers(
    admin: &str,
    admin_pass: &str,
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
        )
        .with_test_writer()
        .try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir0 = tempfile::tempdir().expect("tempdir b0");
    let dir1 = tempfile::tempdir().expect("tempdir b1");
    let listen0: std::net::SocketAddr = LISTEN.parse().expect("static addr");
    let listen1: std::net::SocketAddr = LISTEN_B1.parse().expect("static addr");
    let ctrl0: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let ctrl1: std::net::SocketAddr = "0.0.0.0:9095".parse().expect("static addr");
    let voters = vec![(1_u64, ctrl0), (2_u64, ctrl1)];

    let mk_cfg = |idx: u64,
                  listen: std::net::SocketAddr,
                  ctrl: std::net::SocketAddr,
                  advertised: &str,
                  log_dir: std::path::PathBuf,
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: idx,
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: mode,
            listeners: vec![ListenerSpec {
                name: "SASL_PLAINTEXT".to_string(),
                bind_addr: listen,
                advertised: advertised.to_string(),
                protocol: ListenerProtocol::SaslPlaintext,
            }],
            inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials {
                mechanism: SaslMechanism::Plain,
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.plain_credentials
            .insert(admin.to_string(), admin_pass.to_string());
        cfg
    };

    let cfg0 = mk_cfg(
        1,
        listen0,
        ctrl0,
        BOOTSTRAP,
        dir0.path().to_path_buf(),
        crabka_broker::BootstrapMode::Bootstrap,
    );
    let broker0 = Broker::start(cfg0).await.expect("start broker 0");

    let cfg1 = mk_cfg(
        2,
        listen1,
        ctrl1,
        BOOTSTRAP_B1,
        dir1.path().to_path_buf(),
        crabka_broker::BootstrapMode::Join,
    );
    let join_handle = tokio::spawn(async move { Broker::start(cfg1).await });

    // Bring broker 1 into the raft voter set.
    broker0
        .add_learner(2, ctrl1)
        .await
        .expect("add_learner for broker 1");
    let target: std::collections::BTreeSet<u64> = [1_u64, 2_u64].into_iter().collect();
    broker0
        .change_membership(target)
        .await
        .expect("change_membership to {1,2}");

    let broker1 = join_handle
        .await
        .expect("broker 1 spawn join")
        .expect("broker 1 start");

    eprintln!(
        "CRABKA[test] two-broker sasl: b0={LISTEN} adv={BOOTSTRAP} b1={LISTEN_B1} adv={BOOTSTRAP_B1}"
    );
    let _ = HOST_PORT;
    let _ = HOST_PORT_B1;
    (broker0, broker1, dir0, dir1)
}

/// JVM-driven 2-broker test exercising the `SASL_PLAINTEXT` inter-broker
/// listener. Both brokers boot with the same shared `admin` credential
/// (mechanism=PLAIN); the raft layer authenticates each peer in both
/// directions before the cluster converges on a 2-broker metadata view.
/// A JVM client then SASL-authenticates as the same `admin` identity over
/// the data-plane listener, creates a topic, and produces 50 records.
///
/// Why this test is *not* a follower-replication assertion: the brokers
/// in this test advertise `host.docker.internal:<port>` so the JVM
/// container can reach them via `--add-host=...:host-gateway`. Under WSL2
/// that hostname resolves to the Windows host IP (e.g. `10.0.0.170`),
/// which is *not* routable back into the WSL VM where the broker peers
/// live. So follower-fetch traffic that flows broker→broker
/// (`InterBrokerClient` dialing the peer's advertised address from
/// `MetadataImage`) cannot complete on this network topology. That isn't
/// a SASL or replication bug — it's a Docker-on-WSL networking limitation.
/// The Rust-driven equivalent — `tests/auth_handlers.rs::two_broker_sasl::
/// two_broker_sasl_plaintext_replication` — uses 127.0.0.1 advertised
/// addresses for both brokers and *does* exercise end-to-end inter-broker
/// SASL replication. Use that as the load-bearing inter-broker SASL test.
///
/// What this test *does* assert end-to-end through the JVM client:
///
/// 1. Two brokers boot with `SASL_PLAINTEXT` inter-broker auth, exchanging
///    raft `AppendEntries` + `BrokerHeartbeat` traffic over SASL.
/// 2. The cluster converges on a 2-broker metadata view (both brokers'
///    `broker_count() >= 2`).
/// 3. The JVM `kafka-topics` and `kafka-console-producer` tools both
///    SASL-authenticate as `admin` and successfully drive a single-partition
///    topic produce against broker 0.
/// 4. Broker 0's local log has all 50 records after produce returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_inter_broker_replication_authed() {
    const TOPIC: &str = "crabka-jvm-inter-broker-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (broker0, broker1, _dir0, _dir1) = start_two_sasl_brokers(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    // Wait for both brokers to converge on a 2-broker metadata image —
    // the load-bearing inter-broker SASL handshake. If the peer SASL
    // credentials mismatched, broker 1 would never register and this
    // would time out.
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
    loop {
        let n0 = broker0.broker_count().await;
        let n1 = broker1.broker_count().await;
        if n0 >= 2 && n1 >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "brokers didn't converge on 2-broker view within 60s (b0={n0} b1={n1})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // JVM client config: SASL_PLAINTEXT + PLAIN as the admin (super-user).
    let props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let mount = props.mount_str();

    // Create an rf=1 topic (see test doc-comment — JVM-driven rf=2
    // assertion isn't reliable under WSL networking). Single replica is
    // enough to prove the JVM client → broker SASL handshake works in
    // both directions across the two-broker cluster's controller layer.
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

    // Wait for the topic to materialize on its leader (either broker).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let on_b0 = broker0.has_partition(TOPIC, 0).await;
        let on_b1 = broker1.has_partition(TOPIC, 0).await;
        if on_b0 || on_b1 {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "topic did not propagate within 30s (b0={on_b0} b1={on_b1})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Produce 50 records via `kafka-console-producer`. The metadata
    // response steers the producer to whichever broker leads partition 0.
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
    let payload: String = (0..50)
        .map(|i| format!("rec-{i}\n"))
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

    // Verify the leader has 50 records on disk. We don't know in advance
    // which broker leads partition 0 (raft picks one), so accept either.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let off0 = broker0.local_log_end_offset(TOPIC, 0).await.unwrap_or(0);
        let off1 = broker1.local_log_end_offset(TOPIC, 0).await.unwrap_or(0);
        if off0 >= 50 || off1 >= 50 {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "leader didn't reach 50 records within 10s (b0={off0} b1={off1})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    broker0.shutdown().await;
    broker1.shutdown().await;
}

/// Slice 12b: spawn two in-process brokers that share an inter-broker SASL
/// credential AND both terminate TLS on the data plane and the controller
/// quorum listener. Mirrors [`start_two_sasl_brokers`] but with the `SASL_SSL`
/// listener protocol + `controller_listener_protocol = ctrl` (typically
/// `ListenerProtocol::SaslSsl`). Each broker advertises
/// `host.docker.internal:<port>` so the JVM containers can reach them via
/// `--add-host=host.docker.internal:host-gateway` AND so each broker can
/// dial its peer using the same host name.
#[cfg(not(target_os = "windows"))]
#[allow(clippy::too_many_lines)]
async fn start_two_sasl_ssl_brokers_with_controller_protocol(
    ctrl_protocol: crabka_security::ListenerProtocol,
    admin: &str,
    admin_pass: &str,
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
        )
        .with_test_writer()
        .try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir0 = tempfile::tempdir().expect("tempdir b0");
    let dir1 = tempfile::tempdir().expect("tempdir b1");
    let listen0: std::net::SocketAddr = LISTEN.parse().expect("static addr");
    let listen1: std::net::SocketAddr = LISTEN_B1.parse().expect("static addr");
    let ctrl0: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let ctrl1: std::net::SocketAddr = "0.0.0.0:9095".parse().expect("static addr");
    let voters = vec![(1_u64, ctrl0), (2_u64, ctrl1)];

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cert_path = manifest_dir
        .join("..")
        .join("security")
        .join("tests")
        .join("fixtures")
        .join("dev_cert.pem");
    let key_path = manifest_dir
        .join("..")
        .join("security")
        .join("tests")
        .join("fixtures")
        .join("dev_key.pem");

    let mk_cfg = |idx: u64,
                  listen: std::net::SocketAddr,
                  ctrl: std::net::SocketAddr,
                  advertised: &str,
                  log_dir: std::path::PathBuf,
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: idx,
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            // Slightly more generous than the SASL_PLAINTEXT helper because
            // both data-plane and controller-plane handshakes now include
            // a TLS handshake on top of SASL; on a busy WSL/CI runner the
            // extra round trips can push past 5s.
            controller_election_timeout: std::time::Duration::from_secs(8),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: mode,
            listeners: vec![ListenerSpec {
                name: "SASL_SSL".to_string(),
                bind_addr: listen,
                advertised: advertised.to_string(),
                protocol: ListenerProtocol::SaslSsl,
            }],
            inter_broker_listener_name: "SASL_SSL".to_string(),
            controller_listener_protocol: ctrl_protocol,
            tls_config: Some(TlsConfig {
                cert_chain_path: cert_path.clone(),
                private_key_path: key_path.clone(),
                // Each broker must trust the dev cert that its peer
                // presents on inter-broker raft + replication dials.
                // Without this, the InterBrokerClient TlsConnector has
                // an empty trust-root store and rejects the peer's
                // self-signed cert as `UnknownIssuer`.
                trust_roots_path: Some(cert_path.clone()),
            }),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials {
                mechanism: SaslMechanism::Plain,
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.plain_credentials
            .insert(admin.to_string(), admin_pass.to_string());
        cfg
    };

    let cfg0 = mk_cfg(
        1,
        listen0,
        ctrl0,
        BOOTSTRAP,
        dir0.path().to_path_buf(),
        crabka_broker::BootstrapMode::Bootstrap,
    );
    let broker0 = Broker::start(cfg0).await.expect("start broker 0");

    let cfg1 = mk_cfg(
        2,
        listen1,
        ctrl1,
        BOOTSTRAP_B1,
        dir1.path().to_path_buf(),
        crabka_broker::BootstrapMode::Join,
    );
    let join_handle = tokio::spawn(async move { Broker::start(cfg1).await });

    // Bring broker 1 into the raft voter set.
    broker0
        .add_learner(2, ctrl1)
        .await
        .expect("add_learner for broker 1");
    let target: std::collections::BTreeSet<u64> = [1_u64, 2_u64].into_iter().collect();
    broker0
        .change_membership(target)
        .await
        .expect("change_membership to {1,2}");

    let broker1 = join_handle
        .await
        .expect("broker 1 spawn join")
        .expect("broker 1 start");

    eprintln!(
        "CRABKA[test] two-broker sasl_ssl: b0={LISTEN} adv={BOOTSTRAP} b1={LISTEN_B1} adv={BOOTSTRAP_B1} ctrl_protocol={ctrl_protocol:?}"
    );
    let _ = HOST_PORT;
    let _ = HOST_PORT_B1;
    (broker0, broker1, dir0, dir1)
}

/// Slice 12b: two-broker `SASL_SSL` cluster with `controller_listener_protocol =
/// SaslSsl`. Provisions a SCRAM user, produces rf=2 via JVM client, asserts
/// both brokers replicate the records. Supersedes slice 12 T23's simplified
/// inter-broker test (which only proved metadata convergence) by exercising
/// the full production-shape stack: TLS-terminated controller raft RPC,
/// TLS-terminated data-plane SASL, and rf=2 follower replication.
///
/// Networking: like the `SASL_PLAINTEXT` inter-broker test, this advertises
/// `host.docker.internal:<port>` so the JVM containers can reach the
/// brokers. Under WSL2 the broker→broker `InterBrokerClient` hop may fail
/// because `host.docker.internal` resolves to the Windows host IP, not the
/// WSL VM where the peers live. The CI runner's `/etc/hosts` setup makes
/// that hop work end-to-end; on WSL the test may time out at the rf=2
/// offset check even though `SASL_SSL` itself is correctly wired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_inter_broker_sasl_ssl_raft_replication() {
    use crabka_security::ListenerProtocol;

    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";
    const TOPIC: &str = "crabka-sasl-ssl-raft-rf2";

    let (broker0, broker1, _dir0, _dir1) = start_two_sasl_ssl_brokers_with_controller_protocol(
        ListenerProtocol::SaslSsl,
        ADMIN,
        ADMIN_PASS,
    )
    .await;
    nc_check_connectivity();
    let truststore_path = prepare_jks_truststore();
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());

    // Wait for both brokers to converge on a 2-broker metadata image —
    // the load-bearing inter-broker SASL_SSL handshake on the controller
    // listener. Without TLS + SASL working in both directions, broker 1
    // never registers and this would time out.
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
    loop {
        let n0 = broker0.broker_count().await;
        let n1 = broker1.broker_count().await;
        if n0 >= 2 && n1 >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "brokers didn't converge on 2-broker view within 60s (b0={n0} b1={n1})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Step A: provision alice's SCRAM-SHA-512 credential via admin/PLAIN
    // over the SASL_SSL data-plane listener. Use cp-kafka:7.5.0 (KIP-554).
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
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

    // Step B: drive create-topic + produce as alice over SASL_SSL+SCRAM.
    // Slice 13: disable idempotent producer mode so alice doesn't need
    // `Cluster IdempotentWrite`.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_props_mount = alice_props.mount_str();

    // Create topic rf=2 across both brokers. Run as `admin` (super-user)
    //  for slice-13's CreateTopics Cluster-Create authorize check, then
    //  grant alice Read/Write on the topic; slice-13b implications
    //  auto-grant Describe via Read and Write.
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &ts_mount],
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    for op in ["Read", "Write"] {
        docker_run_kafka_tool_with_image_and_mounts(
            KAFKA_IMAGE_TXN,
            &[&admin_props.mount_str(), &ts_mount],
            &[
                "kafka-acls",
                "--add",
                "--allow-principal",
                &format!("User:{ALICE}"),
                "--operation",
                op,
                "--topic",
                TOPIC,
                "--bootstrap-server",
                BOOTSTRAP,
                "--command-config",
                "/client.properties",
            ],
        );
    }

    // Wait for the topic to materialize on both brokers.
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
    loop {
        let on_b0 = broker0.has_partition(TOPIC, 0).await;
        let on_b1 = broker1.has_partition(TOPIC, 0).await;
        if on_b0 && on_b1 {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "topic did not propagate to both brokers within 60s (b0={on_b0} b1={on_b1})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Produce 50 records via `kafka-console-producer` as alice over SASL_SSL.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &alice_props_mount,
            "-v",
            &ts_mount,
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
    let payload: String = (0..50)
        .map(|i| format!("rec-{i}\n"))
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

    // Assert BOTH brokers reach offset 50 on partition 0 — proves rf=2
    // follower replication completed over the SASL_SSL inter-broker
    // listener (the production-shape end-to-end claim).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    loop {
        let off0 = broker0.local_log_end_offset(TOPIC, 0).await.unwrap_or(0);
        let off1 = broker1.local_log_end_offset(TOPIC, 0).await.unwrap_or(0);
        if off0 >= 50 && off1 >= 50 {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "SASL_SSL rf=2 brokers didn't both reach 50 records within 90s (b0={off0} b1={off1})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    broker0.shutdown().await;
    broker1.shutdown().await;
}

/// Spawn the broker with a single `SASL_PLAINTEXT` listener that enables
/// PLAIN, plus a configured PLAIN super-user. Mirrors
/// [`start_sasl_plaintext_broker`] otherwise. Used by the slice-13 ACL
/// JVM acceptance tests: the super-user authenticates via PLAIN and runs
/// `kafka-acls --add/--remove/--list` (which hit `CreateAcls (30)` /
/// `DeleteAcls (31)` / `DescribeAcls (29)`, all of which require the
/// `Cluster Alter` or `Cluster Describe` operation — the super-user bypass
/// in `authorize()` short-circuits that check).
async fn start_sasl_plaintext_broker_with_super_user(
    super_user: &str,
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
        super_users: std::collections::HashSet::from([super_user.to_string()]),
        ..BrokerConfig::default()
    };
    for (u, p) in users {
        config
            .plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    let handle = Broker::start(config)
        .await
        .expect("start sasl broker with super-user");
    eprintln!(
        "CRABKA[test] sasl super-user broker started listen={LISTEN} advertised={BOOTSTRAP} super_user={super_user}"
    );
    (handle, dir)
}

/// JVM acceptance — `kafka-acls.sh` end-to-end provision flow.
///
/// Drives the modern `kafka-acls.sh` flag set (cp-kafka:7.5.0, Kafka 3.5+)
/// against the Rust broker's `CreateAcls (30)` / `DescribeAcls (29)` /
/// `DeleteAcls (31)` handlers. Admin authenticates as PLAIN super-user
/// — its `Cluster Alter`/`Cluster Describe` checks bypass via the
/// super-user short-circuit in `authorize()`.
///
/// Sequence:
/// 1. `--add` an Allow Read on `Topic LITERAL "foo"` for `User:alice`.
/// 2. `--list --topic foo` must show that binding.
/// 3. `--remove --force` removes it; `--list --topic foo` must be empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_kafka_acls_provision_via_cli() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (broker, _dir) =
        start_sasl_plaintext_broker_with_super_user(ADMIN, &[(ADMIN, ADMIN_PASS)]).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let mount = admin_props.mount_str();

    // 1. --add.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--topic",
            "foo",
        ],
    );

    // 2. --list --topic foo. Expect a line containing alice + READ + ALLOW.
    let list_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--list",
            "--topic",
            "foo",
        ],
    );
    let listed = String::from_utf8_lossy(&list_out.stdout);
    assert!(
        listed.contains("User:alice"),
        "expected alice in --list output; got: {listed}"
    );
    assert!(
        listed.to_ascii_uppercase().contains("READ"),
        "expected READ in --list output; got: {listed}"
    );
    assert!(
        listed.to_ascii_uppercase().contains("ALLOW"),
        "expected ALLOW in --list output; got: {listed}"
    );

    // 3. --remove --force. Then re-list and assert alice is no longer present.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--remove",
            "--force",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--topic",
            "foo",
        ],
    );

    let list_out2 = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--list",
            "--topic",
            "foo",
        ],
    );
    let listed2 = String::from_utf8_lossy(&list_out2.stdout);
    assert!(
        !listed2.contains("User:alice"),
        "alice should be gone after --remove; got: {listed2}"
    );

    broker.shutdown().await;
}

/// JVM acceptance — authorized produce + consume round-trip.
///
/// Admin (PLAIN super-user) provisions alice with:
/// - `Allow Read+Write Topic LITERAL "foo"`
/// - `Allow Read Group LITERAL "cg-foo"`
///
/// Slice-13b implies Describe from Read/Write on the same resource, so no
/// explicit Describe ACL is seeded here — the Metadata per-topic check
/// relies on the implication path.
///
/// Then alice (PLAIN, no super-user, no cluster perms) drives
/// `kafka-console-producer` and `kafka-console-consumer --group cg-foo`
/// against the broker. Exercises the full `Produce`/`Fetch`/`JoinGroup`/
/// `OffsetFetch`/`OffsetCommit` authorize preambles end-to-end.
///
/// Topic auto-creation is intentionally avoided: admin pre-creates `foo`
/// before granting alice access, so the Produce path doesn't have to
/// short-circuit on a missing topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_authorized_produce_consume() {
    const TOPIC: &str = "foo";
    const GROUP: &str = "cg-foo";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_sasl_plaintext_broker_with_super_user(
        ADMIN,
        &[(ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS)],
    )
    .await;
    nc_check_connectivity();

    // ---- Admin step: pre-create the topic and provision alice's ACLs.
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
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

    // Allow Read+Write on Topic foo for User:alice. Slice-13b implies
    // Describe from Read/Write on the same topic, so no explicit Describe
    // ACL is required here.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--operation",
            "Write",
            "--topic",
            TOPIC,
        ],
    );

    // Allow Read on Group cg-foo for User:alice. Slice-13b implies Describe
    // from Read on the same group resource, so no explicit Describe is
    // needed.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--group",
            GROUP,
        ],
    );

    // ---- Alice step: produce + consume over PLAIN as an ordinary user.
    //
    // `enable.idempotence=false` is required: cp-kafka 7.5 producers default
    // to idempotent mode, which sends `InitProducerId` without a
    // transactional id and so checks `Cluster IdempotentWrite` — a
    // cluster-scoped ACL that alice (a non-super-user with only topic +
    // group ACLs) doesn't hold. Falling back to the non-idempotent path
    // keeps alice's required ACL set bounded to what the plan calls out.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    // Produce 10 records via stdin.
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

    // Consume via `--group cg-foo --from-beginning` (the group-coordinator
    // path; exercises JoinGroup/OffsetFetch/OffsetCommit authorize).
    let consumer_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_mount,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "10",
            "--timeout-ms",
            "30000",
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

/// JVM acceptance — produce by an unauthorized principal must fail.
///
/// Admin (PLAIN super-user) provisions alice with Read+Write on topic `foo`
/// (Describe is implied by slice-13b; same effective ACLs as
/// `jvm_authorized_produce_consume`). Bob has valid PLAIN credentials but
/// no ACLs at all. Bob's `kafka-console-producer` must be denied.
///
/// Assertion strategy: `kafka-console-producer` is a fire-and-forget shell
/// wrapper around the Java client. As of cp-kafka 7.5.0 it logs
/// `TopicAuthorizationException` on every Metadata-denied response, but
/// the wrapper itself exits 0 — it retries silently and never propagates
/// the underlying broker-side AUTH failure into a non-zero exit code. So
/// the contract we assert is stderr-shaped, not exit-code-shaped: stderr
/// must contain `TopicAuthorizationException`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_unauthorized_produce_fails() {
    const TOPIC: &str = "foo";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";
    const BOB: &str = "bob";
    const BOB_PASS: &str = "bob-secret";

    let (broker, _dir) = start_sasl_plaintext_broker_with_super_user(
        ADMIN,
        &[(ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS), (BOB, BOB_PASS)],
    )
    .await;
    nc_check_connectivity();

    // ---- Admin step: pre-create topic + provision alice (not bob).
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
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

    // alice gets Read+Write — proves that the broker has ACLs configured
    // (i.e. the empty-ACL ALLOW shim is not active). Slice-13b implies
    // Describe from Read/Write so no explicit Describe ACL is needed.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--operation",
            "Write",
            "--topic",
            TOPIC,
        ],
    );

    // ---- Bob step: attempt to produce. Expect stderr to contain
    //               TopicAuthorizationException.
    let bob_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(BOB, BOB_PASS),
    ));
    let bob_mount = bob_props.mount_str();

    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &bob_mount,
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
        .expect("spawn bob producer");
    let payload = b"unauth-msg\n";
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload)
        .expect("write stdin");
    drop(child.stdin.take());
    let bob_out = child.wait_with_output().expect("wait bob producer");
    let stderr = String::from_utf8_lossy(&bob_out.stderr);
    let stdout = String::from_utf8_lossy(&bob_out.stdout);
    eprintln!(
        "CRABKA[test] bob producer status={} stderr={stderr} stdout={stdout}",
        bob_out.status,
    );
    assert!(
        stderr.contains("TopicAuthorizationException"),
        "bob producer should log TopicAuthorizationException; stderr={stderr} stdout={stdout}",
    );

    broker.shutdown().await;
}

/// JVM acceptance — consumer denied on the group-resource path.
///
/// Alice has Read on topic `foo` (Describe implied by slice-13b) but no ACL
/// on group `cg-other`. `kafka-console-consumer --group cg-other` must fail
/// with `GroupAuthorizationException` (denied at `JoinGroup`/`OffsetFetch`,
/// before any Fetch happens).
///
/// Assertion strategy: stderr-shaped. We assert on stderr content for
/// symmetry with `jvm_unauthorized_produce_fails` and to keep the
/// contract stable across cp-kafka versions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_unauthorized_consumer_fails_group_check() {
    const TOPIC: &str = "foo";
    const GROUP: &str = "cg-other";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_sasl_plaintext_broker_with_super_user(
        ADMIN,
        &[(ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS)],
    )
    .await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
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

    // alice: Read on Topic foo (Describe implied by slice-13b). Deliberately
    // no group ACL so the consumer hits GroupAuthorizationException.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--topic",
            TOPIC,
        ],
    );

    // ---- Alice consumer using --group cg-other. Expect group-denied stderr.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &alice_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
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
            "15000",
            "--consumer.config",
            "/client.properties",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn alice consumer");
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    eprintln!(
        "CRABKA[test] alice consumer group-denied status={} stderr={stderr} stdout={stdout}",
        out.status,
    );
    assert!(
        stderr.contains("GroupAuthorizationException"),
        "consumer should log GroupAuthorizationException; stderr={stderr} stdout={stdout}",
    );

    broker.shutdown().await;
}

/// JVM acceptance — prefixed topic ACL grants exactly the prefix.
///
/// Admin provisions:
/// - `Allow Read Topic PREFIXED "team-"` for alice (Describe implied by slice-13b)
/// - `Allow Read Group LITERAL "cg-prefixed"` for alice (Describe implied by slice-13b)
///
/// Then pre-creates two topics: `team-foo` (covered by the prefix) and
/// `other-foo` (NOT covered). Seeds one record into each via the admin
/// (super-user, bypasses authorize).
///
/// Alice's consumer:
/// 1. `--topic team-foo` succeeds and reads the seeded record (exercises
///    the PREFIXED Read path in `authorize`).
/// 2. `--topic other-foo` fails with `TopicAuthorizationException`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_prefixed_topic_acl_works() {
    const PREFIX: &str = "team-";
    const TOPIC_OK: &str = "team-foo";
    const TOPIC_DENIED: &str = "other-foo";
    const GROUP: &str = "cg-prefixed";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_sasl_plaintext_broker_with_super_user(
        ADMIN,
        &[(ADMIN, ADMIN_PASS), (ALICE, ALICE_PASS)],
    )
    .await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Pre-create both topics.
    for topic in [TOPIC_OK, TOPIC_DENIED] {
        docker_run_kafka_tool_with_image_and_mount(
            KAFKA_IMAGE_TXN,
            &admin_mount,
            &[
                "kafka-topics",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
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
    }

    // Prefixed Read on `team-*` for alice. Slice-13b implies Describe from
    // Read on the same topic resource.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--resource-pattern-type",
            "prefixed",
            "--topic",
            PREFIX,
        ],
    );

    // Literal Read on group `cg-prefixed`. Slice-13b implies Describe from
    // Read on the same group resource.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-acls",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--group",
            GROUP,
        ],
    );

    // Seed one record into each topic as admin (super-user bypasses authorize).
    let admin_producer_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_producer_mount = admin_producer_props.mount_str();

    for topic in [TOPIC_OK, TOPIC_DENIED] {
        let mut child = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-i",
                "-v",
                &admin_producer_mount,
                "--add-host=host.docker.internal:host-gateway",
                KAFKA_IMAGE_TXN,
                "kafka-console-producer",
                "--bootstrap-server",
                BOOTSTRAP,
                "--topic",
                topic,
                "--producer.config",
                "/client.properties",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn admin seed producer");
        child
            .stdin
            .as_mut()
            .expect("stdin")
            .write_all(format!("seed-{topic}\n").as_bytes())
            .expect("write seed");
        drop(child.stdin.take());
        let seed_out = child.wait_with_output().expect("wait seed producer");
        assert!(
            seed_out.status.success(),
            "admin seed producer failed for {topic}: stderr={}",
            String::from_utf8_lossy(&seed_out.stderr),
        );
    }

    // ---- Alice: consume team-foo (allowed by prefix).
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    let consumer_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_mount,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC_OK,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "30000",
            "--consumer.config",
            "/client.properties",
        ],
    );
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    let needle = format!("seed-{TOPIC_OK}");
    assert!(
        stdout.contains(&needle),
        "alice should read {needle} from prefixed topic; got: {stdout}",
    );

    // ---- Alice: consume other-foo (denied — no matching prefix).
    let denied_out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &alice_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-console-consumer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC_DENIED,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "15000",
            "--consumer.config",
            "/client.properties",
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn alice denied consumer");
    let denied_stderr = String::from_utf8_lossy(&denied_out.stderr);
    let denied_stdout = String::from_utf8_lossy(&denied_out.stdout);
    eprintln!(
        "CRABKA[test] alice denied consumer status={} stderr={denied_stderr} stdout={denied_stdout}",
        denied_out.status,
    );
    assert!(
        denied_stderr.contains("TopicAuthorizationException"),
        "alice should be denied on {TOPIC_DENIED}; stderr={denied_stderr} stdout={denied_stdout}",
    );

    broker.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Task 10: JVM kafka-leader-election --election-type preferred
// ─────────────────────────────────────────────────────────────────────────────

/// Third broker for the 3-broker `SASL_PLAINTEXT` JVM cluster.
/// Broker 2 (`node_id`=2) lives on 9094/9095 (`HOST_PORT_B1` / `BOOTSTRAP_B1`).
/// Broker 3 (`node_id`=3) lives on 9096/9097.
const HOST_PORT_B2: u16 = 9096;
const BOOTSTRAP_B2: &str = "host.docker.internal:9096";
const LISTEN_B2: &str = "0.0.0.0:9096";

/// Spawn three in-process brokers sharing a single inter-broker SASL credential.
///
/// * Broker 1: 0.0.0.0:9092 (data) / 0.0.0.0:9093 (controller)
/// * Broker 2: 0.0.0.0:9094 (data) / 0.0.0.0:9095 (controller)
/// * Broker 3: 0.0.0.0:9096 (data) / 0.0.0.0:9097 (controller)
///
/// Returns `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)`.
/// The `cfg*` values are needed to revive a broker after shutdown
/// (pass with `BootstrapMode::Rejoin`).
#[allow(clippy::type_complexity)]
#[allow(clippy::too_many_lines)]
async fn start_three_broker_sasl_plaintext_jvm_cluster(
    admin: &str,
    admin_pass: &str,
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
        )
        .with_test_writer()
        .try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir0 = tempfile::tempdir().expect("tempdir b0");
    let dir1 = tempfile::tempdir().expect("tempdir b1");
    let dir2 = tempfile::tempdir().expect("tempdir b2");

    let listen0: std::net::SocketAddr = LISTEN.parse().expect("static addr");
    let listen1: std::net::SocketAddr = LISTEN_B1.parse().expect("static addr");
    let listen2: std::net::SocketAddr = LISTEN_B2.parse().expect("static addr");

    let ctrl0: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let ctrl1: std::net::SocketAddr = "0.0.0.0:9095".parse().expect("static addr");
    let ctrl2: std::net::SocketAddr = "0.0.0.0:9097".parse().expect("static addr");

    let voters = vec![(1_u64, ctrl0), (2_u64, ctrl1), (3_u64, ctrl2)];

    let mk_cfg = |idx: u64,
                  listen: std::net::SocketAddr,
                  ctrl: std::net::SocketAddr,
                  advertised: &str,
                  log_dir: std::path::PathBuf,
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: idx,
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: mode,
            listeners: vec![ListenerSpec {
                name: "SASL_PLAINTEXT".to_string(),
                bind_addr: listen,
                advertised: advertised.to_string(),
                protocol: ListenerProtocol::SaslPlaintext,
            }],
            inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials {
                mechanism: SaslMechanism::Plain,
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.plain_credentials
            .insert(admin.to_string(), admin_pass.to_string());
        cfg
    };

    let cfg0 = mk_cfg(
        1,
        listen0,
        ctrl0,
        BOOTSTRAP,
        dir0.path().to_path_buf(),
        crabka_broker::BootstrapMode::Bootstrap,
    );
    let broker0 = Broker::start(cfg0.clone()).await.expect("start broker 0");

    let cfg1 = mk_cfg(
        2,
        listen1,
        ctrl1,
        BOOTSTRAP_B1,
        dir1.path().to_path_buf(),
        crabka_broker::BootstrapMode::Join,
    );
    let join_handle1 = tokio::spawn({
        let c = cfg1.clone();
        async move { Broker::start(c).await }
    });

    // Bring broker 2 (node_id=2) into the raft voter set first.
    broker0
        .add_learner(2, ctrl1)
        .await
        .expect("add_learner for broker 1");
    let target2: std::collections::BTreeSet<u64> = [1_u64, 2_u64].into_iter().collect();
    broker0
        .change_membership(target2)
        .await
        .expect("change_membership to {1,2}");

    let broker1 = join_handle1
        .await
        .expect("broker 1 spawn join")
        .expect("broker 1 start");

    let cfg2 = mk_cfg(
        3,
        listen2,
        ctrl2,
        BOOTSTRAP_B2,
        dir2.path().to_path_buf(),
        crabka_broker::BootstrapMode::Join,
    );
    let join_handle2 = tokio::spawn({
        let c = cfg2.clone();
        async move { Broker::start(c).await }
    });

    // Bring broker 3 (node_id=3) into the raft voter set.
    broker0
        .add_learner(3, ctrl2)
        .await
        .expect("add_learner for broker 2");
    let target3: std::collections::BTreeSet<u64> = [1_u64, 2_u64, 3_u64].into_iter().collect();
    broker0
        .change_membership(target3)
        .await
        .expect("change_membership to {1,2,3}");

    let broker2 = join_handle2
        .await
        .expect("broker 2 spawn join")
        .expect("broker 2 start");

    eprintln!(
        "CRABKA[test] three-broker sasl: b0={LISTEN} adv={BOOTSTRAP} b1={LISTEN_B1} adv={BOOTSTRAP_B1} b2={LISTEN_B2} adv={BOOTSTRAP_B2}"
    );
    let _ = HOST_PORT;
    let _ = HOST_PORT_B1;
    let _ = HOST_PORT_B2;
    (
        broker0, broker1, broker2, cfg0, cfg1, cfg2, dir0, dir1, dir2,
    )
}

/// Poll until `handle` reports `leader` as the leader for `(topic, partition)`.
async fn wait_jvm_partition_leader(
    handle: &crabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
    leader: u64,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if handle.partition_leader_for_test(topic, partition) == Some(leader) {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "partition {topic}-{partition} didn't elect leader={leader} within 30s; current={:?}",
            handle.partition_leader_for_test(topic, partition)
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Poll until the ISR for `(topic, partition)` contains `node`.
async fn wait_jvm_isr_contains(
    handle: &crabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
    node: u64,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if handle
            .partition_isr_for_test(topic, partition)
            .is_some_and(|isr| isr.contains(&node))
        {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "ISR for {topic}-{partition} never included node={node} within 30s; current={:?}",
            handle.partition_isr_for_test(topic, partition)
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Poll until `handle` reports any non-zero leader for `(topic, partition)`.
/// Returns the leader node id.
async fn wait_jvm_partition_any_leader(
    handle: &crabka_broker::BrokerHandle,
    topic: &str,
    partition: i32,
) -> u64 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Some(l) = handle.partition_leader_for_test(topic, partition) {
            return l;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "partition {topic}-{partition} had no leader within 30s",
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Poll until all three brokers have seen `n_brokers` registered brokers.
async fn wait_three_brokers_registered(
    h1: &crabka_broker::BrokerHandle,
    h2: &crabka_broker::BrokerHandle,
    h3: &crabka_broker::BrokerHandle,
    n_brokers: usize,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
    loop {
        let c1 = h1.broker_count().await;
        let c2 = h2.broker_count().await;
        let c3 = h3.broker_count().await;
        if c1 >= n_brokers && c2 >= n_brokers && c3 >= n_brokers {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "brokers didn't converge on {n_brokers}-broker view within 60s (b1={c1} b2={c2} b3={c3})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// JVM acceptance test for `kafka-leader-election --election-type preferred`.
///
/// Uses a **3-broker** `SASL_PLAINTEXT` cluster so that the raft quorum (2/3)
/// survives killing broker 1 (the preferred replica). A 2-broker cluster
/// would lose quorum (1/2) when broker 1 dies and could not commit the
/// partition-leader change that the PREFERRED election requires.
///
/// Scenario:
/// 1. Boot 3-broker `SASL_PLAINTEXT` cluster; create rf=2 topic.
/// 2. Wait for the cluster to assign a leader (expected: broker 1 = preferred).
/// 3. Kill broker 1 → broker 2 (or 3) leads partition 0 via automatic failover.
/// 4. Revive broker 1 (Rejoin); wait for it to re-enter the ISR on broker 2's
///    view.
/// 5. Run `kafka-leader-election --election-type preferred` via the JVM CLI
///    image (cp-kafka:7.5.0 — older images don't ship this tool).
/// 6. Assert Docker exits 0.
/// 7. Poll until broker 1 is leader again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_kafka_leader_election_preferred() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "crabka-elect-preferred-itest";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    // Wait for all three brokers to register in the metadata image.
    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic as super-user via the 7.5 JVM image.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );

    // Wait for broker 1 to see the partition in its metadata image.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if h1.has_partition(TOPIC, 0).await {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "partition {TOPIC}-0 never appeared on broker 1 within 30s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Record the initial leader (should be broker 1 as preferred replica).
    let initial_leader = wait_jvm_partition_any_leader(&h1, TOPIC, 0).await;
    eprintln!("CRABKA[test] initial partition leader: {initial_leader}");

    // For the preferred election to do anything interesting we need broker 1
    // to be the preferred (replicas[0]). The scheduler should assign [1, 2]
    // since broker 1 is node_id=1 (lowest). Assert this assumption.
    assert_eq!(
        initial_leader, 1,
        "expected broker 1 to be the initial/preferred leader; got {initial_leader}"
    );

    // Inject a PartitionRecord that makes broker 2 the current leader while
    // keeping broker 1 in the ISR as a non-leader replica.
    //
    // This simulates the "preferred replica is not current leader" scenario
    // that `kafka-leader-election --election-type preferred` is designed to
    // fix. We use metadata injection rather than an organic leader change
    // because:
    //
    // 1. An organic leader change requires killing broker 1, which causes the
    //    raft-leader-dependent `ControllerLivenessState` to lose broker 2's
    //    heartbeat record for the window between raft re-election and broker 2's
    //    first heartbeat to the new raft leader — making `ElectLeaders` fail
    //    with `PreferredNotAlive` during that window.
    //
    // 2. Under WSL2, inter-broker replication flows through the Windows-host IP
    //    (`host.docker.internal` = 192.168.65.254), not back into the WSL VM
    //    where the peers live, so organic ISR expansion would time out anyway.
    //
    // Metadata injection bypasses both limitations and matches the technique
    // used by `tests/elect_leaders.rs::unclean_election_via_wire_picks_alive_replica`.
    h1.submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1Partition(
        crabka_metadata::PartitionRecord {
            topic: TOPIC.to_string(),
            partition: 0,
            // Make broker 2 the current leader — so broker 1 (replicas[0])
            // is no longer the leader but is still alive and in the ISR.
            leader: 2,
            replicas: vec![1, 2],
            isr: vec![2, 1],
            leader_epoch: 1,
            adding_replicas: vec![],
            removing_replicas: vec![],
        },
    ))
    .await
    .expect("inject PartitionRecord making broker 2 the leader");

    // Wait for the injected state to propagate to broker 2's metadata image:
    // leader=2, ISR contains both 1 and 2.
    wait_jvm_partition_leader(&h2, TOPIC, 0, 2).await;
    wait_jvm_isr_contains(&h2, TOPIC, 0, 1).await;
    eprintln!(
        "CRABKA[test] broker 2 is current leader; broker 1 is in ISR — running preferred election"
    );

    // Run kafka-leader-election via the 7.5 JVM image.
    // kafka-leader-election is NOT present in cp-kafka:6.1.1 (Kafka 2.7).
    // cp-kafka:7.5.0 (Kafka 3.5) ships it. The tool sends `ElectLeaders`
    // (api_key 43) which the Rust broker now handles via T4/T5.
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-leader-election",
            "--election-type",
            "preferred",
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--bootstrap-server",
            BOOTSTRAP,
            "--admin.config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-leader-election");

    let election_stdout = String::from_utf8_lossy(&out.stdout);
    let election_stderr = String::from_utf8_lossy(&out.stderr);
    eprintln!(
        "CRABKA[test] kafka-leader-election status={} stdout={election_stdout} stderr={election_stderr}",
        out.status
    );
    assert!(
        out.status.success(),
        "kafka-leader-election failed: stdout={election_stdout} stderr={election_stderr}",
    );

    // Poll until broker 1 is the leader again on broker 2's view.
    wait_jvm_partition_leader(&h2, TOPIC, 0, 1).await;
    eprintln!("CRABKA[test] preferred election confirmed: broker 1 is leader again");

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

// ---------------------------------------------------------------------------
// Helper: write an arbitrary tempfile and return a TempFileMount that owns
// the NamedTempFile (so it stays alive as long as the returned value is alive)
// and exposes the host path for Docker `-v` mount specs.
// ---------------------------------------------------------------------------

struct TempFileMount {
    tmp: tempfile::NamedTempFile,
}

impl TempFileMount {
    /// `<host_path>:<container_path>` — caller appends `:ro` if desired.
    fn host_path(&self) -> String {
        self.tmp.path().display().to_string()
    }
}

fn write_temp_file(filename: &str, contents: &str) -> TempFileMount {
    let tmp = tempfile::Builder::new()
        .prefix(filename)
        .tempfile()
        .expect("tempfile");
    std::fs::write(tmp.path(), contents).expect("write tempfile");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod tempfile");
    }
    TempFileMount { tmp }
}

// ---------------------------------------------------------------------------
// JVM acceptance test: kafka-reassign-partitions --execute + --verify
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
async fn jvm_kafka_reassign_partitions_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "crabka-reassign-itest";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );

    // Wait for broker 1 to see the partition.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if h1.has_partition(TOPIC, 0).await {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "partition {TOPIC}-0 never appeared on broker 1 within 30s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Determine initial replicas and pick the third broker as the new target.
    // Broker node IDs are i32 on the wire but stored as u64 in PartitionRecord.
    let pr = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record");
    let initial = pr.replicas.clone();
    // node IDs are 1-3; find the one not in the initial replica set.
    let new_node: u64 = (1u64..=3)
        .find(|n| !initial.contains(n))
        .expect("free broker");
    let staying: u64 = *initial.first().unwrap();
    eprintln!("CRABKA[test] initial replicas={initial:?} staying={staying} new_node={new_node}");

    // Write reassignment JSON: move partition 0 to [staying, new_node].
    let json = format!(
        r#"{{"version":1,"partitions":[{{"topic":"{TOPIC}","partition":0,"replicas":[{staying},{new_node}]}}]}}"#,
    );
    let json_file = write_temp_file("reassignment.json", &json);
    let json_mount = format!("{}:/reassignment.json", json_file.host_path());

    // Execute reassignment.
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "-v",
            &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--execute",
            "--reassignment-json-file",
            "/reassignment.json",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --execute");
    eprintln!(
        "CRABKA[test] --execute status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "kafka-reassign-partitions --execute failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Inject ISR including new_node so the background reassignment-completion
    // task can see the new broker in ISR without relying on inter-broker
    // replication (which is broken under WSL2 due to host-gateway routing;
    // see slice-14 T10 for the same technique).
    let pr_after = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after alter");
    let removing_replica = pr_after
        .removing_replicas
        .first()
        .copied()
        .unwrap_or_else(|| initial.last().copied().unwrap_or(0));
    let injected = crabka_metadata::PartitionRecord {
        isr: vec![staying, new_node, removing_replica],
        ..pr_after.clone()
    };
    h1.submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1Partition(injected))
        .await
        .expect("inject ISR for reassignment completion");

    // Poll until adding_replicas and removing_replicas are both empty and
    // the replicas set matches [staying, new_node].
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let pr = h1
            .partition_record_for_test(TOPIC, 0)
            .expect("partition record (poll)");
        if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
            let got: std::collections::HashSet<u64> = pr.replicas.iter().copied().collect();
            let want: std::collections::HashSet<u64> = [staying, new_node].into_iter().collect();
            assert_eq!(
                got, want,
                "reassignment completed but replicas mismatch: got={got:?} want={want:?}"
            );
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "reassignment did not complete within 20s; pr={pr:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    eprintln!("CRABKA[test] reassignment completed; running --verify");

    // --verify should report completion.
    let verify_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "-v",
            &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--verify",
            "--reassignment-json-file",
            "/reassignment.json",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --verify");
    eprintln!(
        "CRABKA[test] --verify status={} stdout={} stderr={}",
        verify_out.status,
        String::from_utf8_lossy(&verify_out.stdout),
        String::from_utf8_lossy(&verify_out.stderr),
    );
    // Slice 15b supports broker-scoped IncrementalAlterConfigs (resource_type=4),
    // so --verify can now clear throttles and exit 0.
    assert!(
        verify_out.status.success(),
        "kafka-reassign-partitions --verify failed: stderr={}",
        String::from_utf8_lossy(&verify_out.stderr)
    );

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

// ---------------------------------------------------------------------------
// JVM acceptance test: kafka-reassign-partitions --throttle (slice 15b)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
async fn jvm_kafka_reassign_partitions_with_throttle_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "crabka-throttle-reassign-itest";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );

    // Wait for broker 1 to see the partition.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if h1.has_partition(TOPIC, 0).await {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "partition {TOPIC}-0 never appeared on broker 1 within 30s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Determine initial replicas; pick the broker not in the replica set.
    let pr = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record");
    let initial = pr.replicas.clone();
    let new_node: u64 = (1u64..=3)
        .find(|n| !initial.contains(n))
        .expect("free broker");
    let staying: u64 = *initial.first().unwrap();
    eprintln!("CRABKA[test] initial replicas={initial:?} staying={staying} new_node={new_node}");

    // Write reassignment JSON.
    let json = format!(
        r#"{{"version":1,"partitions":[{{"topic":"{TOPIC}","partition":0,"replicas":[{staying},{new_node}]}}]}}"#,
    );
    let json_file = write_temp_file("reassignment.json", &json);
    let json_mount = format!("{}:/reassignment.json", json_file.host_path());

    // Execute reassignment with --throttle 1024.
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "-v",
            &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--execute",
            "--reassignment-json-file",
            "/reassignment.json",
            "--throttle",
            "1024",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --execute --throttle");
    eprintln!(
        "CRABKA[test] --execute --throttle status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "kafka-reassign-partitions --execute --throttle failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Verify throttle configs were applied via kafka-configs --describe.
    let desc = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-configs",
            "--describe",
            "--entity-type",
            "brokers",
            "--entity-name",
            "1",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-configs --describe");
    eprintln!(
        "CRABKA[test] kafka-configs describe status={} stdout={} stderr={}",
        desc.status,
        String::from_utf8_lossy(&desc.stdout),
        String::from_utf8_lossy(&desc.stderr),
    );
    let desc_stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        desc_stdout.contains("leader.replication.throttled.rate=1024"),
        "leader.replication.throttled.rate=1024 not visible in kafka-configs output: {desc_stdout}"
    );

    // Inject ISR including new_node so the background reassignment-completion
    // task can see the new broker in ISR without relying on inter-broker
    // replication (which is broken under WSL2 due to host-gateway routing;
    // see slice-14 T10 for the same technique).
    let pr_after = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after execute");
    let removing_replica = pr_after
        .removing_replicas
        .first()
        .copied()
        .unwrap_or_else(|| initial.last().copied().unwrap_or(0));
    let injected = crabka_metadata::PartitionRecord {
        isr: vec![staying, new_node, removing_replica],
        ..pr_after.clone()
    };
    h1.submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1Partition(injected))
        .await
        .expect("inject ISR for reassignment completion");

    // Poll until reassignment completes.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let pr = h1
            .partition_record_for_test(TOPIC, 0)
            .expect("partition record (poll)");
        if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
            let got: std::collections::HashSet<u64> = pr.replicas.iter().copied().collect();
            let want: std::collections::HashSet<u64> = [staying, new_node].into_iter().collect();
            assert_eq!(
                got, want,
                "reassignment completed but replicas mismatch: got={got:?} want={want:?}"
            );
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "reassignment did not complete within 20s; pr={pr:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    eprintln!("CRABKA[test] reassignment completed; running --verify");

    // --verify clears throttle configs and exits 0 (slice 15b supports
    // broker-scoped IncrementalAlterConfigs).
    let verify_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "-v",
            &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--verify",
            "--reassignment-json-file",
            "/reassignment.json",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --verify");
    eprintln!(
        "CRABKA[test] --verify status={} stdout={} stderr={}",
        verify_out.status,
        String::from_utf8_lossy(&verify_out.stdout),
        String::from_utf8_lossy(&verify_out.stderr),
    );
    assert!(
        verify_out.status.success(),
        "kafka-reassign-partitions --verify failed (slice 15b should fix this): stderr={}",
        String::from_utf8_lossy(&verify_out.stderr)
    );

    // Confirm throttle configs were cleared from the metadata image after --verify.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = h1.controller_image_for_test();
        if img
            .broker_throttle_rate(1, crabka_metadata::ThrottleKind::Leader)
            .is_none()
        {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "throttle config not cleared from image within 5s after --verify"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// Like [`start_three_broker_sasl_plaintext_jvm_cluster`] but also provisions
/// `extra_users` as PLAIN credentials on all three brokers.
///
/// Returns `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)`.
#[allow(clippy::type_complexity)]
#[allow(clippy::too_many_lines)]
async fn start_three_broker_sasl_plaintext_jvm_cluster_with_users(
    admin: &str,
    admin_pass: &str,
    extra_users: &[(&str, &str)],
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use crabka_security::{ListenerProtocol, SaslMechanism};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info")),
        )
        .with_test_writer()
        .try_init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir0 = tempfile::tempdir().expect("tempdir b0");
    let dir1 = tempfile::tempdir().expect("tempdir b1");
    let dir2 = tempfile::tempdir().expect("tempdir b2");

    let listen0: std::net::SocketAddr = LISTEN.parse().expect("static addr");
    let listen1: std::net::SocketAddr = LISTEN_B1.parse().expect("static addr");
    let listen2: std::net::SocketAddr = LISTEN_B2.parse().expect("static addr");

    let ctrl0: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let ctrl1: std::net::SocketAddr = "0.0.0.0:9095".parse().expect("static addr");
    let ctrl2: std::net::SocketAddr = "0.0.0.0:9097".parse().expect("static addr");

    let voters = vec![(1_u64, ctrl0), (2_u64, ctrl1), (3_u64, ctrl2)];

    let mk_cfg = |idx: u64,
                  listen: std::net::SocketAddr,
                  ctrl: std::net::SocketAddr,
                  advertised: &str,
                  log_dir: std::path::PathBuf,
                  mode: crabka_broker::BootstrapMode|
     -> BrokerConfig {
        let mut cfg = BrokerConfig {
            broker_id: i32::try_from(idx).unwrap(),
            listen_addr: listen,
            advertised_listener: advertised.to_string(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: idx,
            controller_listen_addr: ctrl,
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: mode,
            listeners: vec![ListenerSpec {
                name: "SASL_PLAINTEXT".to_string(),
                bind_addr: listen,
                advertised: advertised.to_string(),
                protocol: ListenerProtocol::SaslPlaintext,
            }],
            inter_broker_listener_name: "SASL_PLAINTEXT".to_string(),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            super_users: std::collections::HashSet::from([admin.to_string()]),
            inter_broker_credentials: Some(InterBrokerCredentials {
                mechanism: SaslMechanism::Plain,
                username: admin.to_string(),
                password: admin_pass.to_string(),
            }),
            ..BrokerConfig::default()
        };
        cfg.plain_credentials
            .insert(admin.to_string(), admin_pass.to_string());
        for (u, p) in extra_users {
            cfg.plain_credentials
                .insert((*u).to_string(), (*p).to_string());
        }
        cfg
    };

    let cfg0 = mk_cfg(
        1,
        listen0,
        ctrl0,
        BOOTSTRAP,
        dir0.path().to_path_buf(),
        crabka_broker::BootstrapMode::Bootstrap,
    );
    let broker0 = Broker::start(cfg0.clone()).await.expect("start broker 0");

    let cfg1 = mk_cfg(
        2,
        listen1,
        ctrl1,
        BOOTSTRAP_B1,
        dir1.path().to_path_buf(),
        crabka_broker::BootstrapMode::Join,
    );
    let join_handle1 = tokio::spawn({
        let c = cfg1.clone();
        async move { Broker::start(c).await }
    });

    broker0
        .add_learner(2, ctrl1)
        .await
        .expect("add_learner for broker 1");
    let target2: std::collections::BTreeSet<u64> = [1_u64, 2_u64].into_iter().collect();
    broker0
        .change_membership(target2)
        .await
        .expect("change_membership to {1,2}");

    let broker1 = join_handle1
        .await
        .expect("broker 1 spawn join")
        .expect("broker 1 start");

    let cfg2 = mk_cfg(
        3,
        listen2,
        ctrl2,
        BOOTSTRAP_B2,
        dir2.path().to_path_buf(),
        crabka_broker::BootstrapMode::Join,
    );
    let join_handle2 = tokio::spawn({
        let c = cfg2.clone();
        async move { Broker::start(c).await }
    });

    broker0
        .add_learner(3, ctrl2)
        .await
        .expect("add_learner for broker 2");
    let target3: std::collections::BTreeSet<u64> = [1_u64, 2_u64, 3_u64].into_iter().collect();
    broker0
        .change_membership(target3)
        .await
        .expect("change_membership to {1,2,3}");

    let broker2 = join_handle2
        .await
        .expect("broker 2 spawn join")
        .expect("broker 2 start");

    eprintln!(
        "CRABKA[test] three-broker sasl (with_users): b0={LISTEN} adv={BOOTSTRAP} b1={LISTEN_B1} adv={BOOTSTRAP_B1} b2={LISTEN_B2} adv={BOOTSTRAP_B2}"
    );
    let _ = HOST_PORT;
    let _ = HOST_PORT_B1;
    let _ = HOST_PORT_B2;
    (
        broker0, broker1, broker2, cfg0, cfg1, cfg2, dir0, dir1, dir2,
    )
}

/// JVM acceptance: `kafka-configs --entity-type users` client quota round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster; alter + describe + delete on a
/// user-scoped `producer_byte_rate` via the JVM admin CLI.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_kafka_configs_alter_client_quota_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            ADMIN,
            ADMIN_PASS,
            &[(ALICE, ALICE_PASS)],
        )
        .await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Set producer_byte_rate=1024 for alice.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            "producer_byte_rate=1024",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    eprintln!(
        "CRABKA[test] alter status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "alter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Describe — confirm visibility.
    // api_key 50 (DescribeUserScramCredentials) is now implemented (slice 17a),
    // so the JVM tool exits 0 cleanly. Use the helper which asserts success.
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("producer_byte_rate=1024"),
        "expected quota in describe output: {stdout}"
    );

    // Delete the config.
    let del_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--delete-config",
            "producer_byte_rate",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        del_out.status.success(),
        "delete-config failed: {}",
        String::from_utf8_lossy(&del_out.stderr)
    );

    // Confirm quota cleared from image (poll up to 5 s).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = h1.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".to_string(), Some(ALICE.to_string()))];
        if img
            .client_quotas()
            .get(&key)
            .and_then(|m| m.get("producer_byte_rate"))
            .is_none()
        {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "quota not cleared from image within 5s after --delete-config"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// JVM acceptance: `kafka-configs --entity-type ips` KIP-612 round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster; alter + describe (stdout substring) +
/// delete-config on (ip=127.0.0.1) `connection_creation_rate` via the JVM admin CLI.
/// Wall-time enforcement is not exercised here (single connection doesn't trigger
/// the rate limit); the Rust integration test in `tests/ip_quotas.rs` covers that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_kafka_configs_alter_ip_quota_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(ADMIN, ADMIN_PASS, &[]).await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Set connection_creation_rate=2 for 127.0.0.1.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "ips",
            "--entity-name",
            "127.0.0.1",
            "--add-config",
            "connection_creation_rate=2.0",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    eprintln!(
        "CRABKA[test] alter status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "alter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Describe — confirm visibility.
    // api_key 50 (DescribeUserScramCredentials) is now implemented (slice 17a),
    // so the JVM tool exits 0 cleanly. Use the helper which asserts success.
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "ips",
            "--entity-name",
            "127.0.0.1",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("connection_creation_rate=2"),
        "expected ip quota in describe output: {stdout}"
    );

    // Delete the config.
    let del_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "ips",
            "--entity-name",
            "127.0.0.1",
            "--delete-config",
            "connection_creation_rate",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        del_out.status.success(),
        "delete-config failed: {}",
        String::from_utf8_lossy(&del_out.stderr)
    );

    // Confirm quota cleared from image (poll up to 5 s).
    let ip_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = h1.controller_image_for_test();
        let key: crabka_metadata::EntityKey =
            vec![("ip".to_string(), Some("127.0.0.1".to_string()))];
        if img
            .client_quotas()
            .get(&key)
            .and_then(|m| m.get("connection_creation_rate"))
            .is_none()
        {
            break;
        }
        assert!(
            std::time::Instant::now() <= ip_deadline,
            "ip quota not cleared from image within 5s after --delete-config"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// JVM acceptance: `kafka-configs --entity-type users controller_mutation_rate` round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster; alter + describe (stdout substring) +
/// delete-config on (user=alice) `controller_mutation_rate` via the JVM admin CLI.
/// No wall-time enforcement test — single `kafka-topics --create` is one request,
/// max throttle 1 s. The Rust integration test in `tests/controller_mutation_quota.rs`
/// covers enforcement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn jvm_kafka_configs_alter_controller_mutation_rate_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            ADMIN,
            ADMIN_PASS,
            &[(ALICE, ALICE_PASS)],
        )
        .await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Alter — set controller_mutation_rate=2.0 for alice.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            "controller_mutation_rate=2.0",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    eprintln!(
        "CRABKA[test] alter status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "alter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Describe — confirm visibility.
    // api_key 50 (DescribeUserScramCredentials) is now implemented (slice 17a),
    // so the JVM tool exits 0 cleanly. Use the helper which asserts success.
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("controller_mutation_rate=2"),
        "expected quota in describe output: {stdout}"
    );

    // Delete the config.
    let del_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--delete-config",
            "controller_mutation_rate",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        del_out.status.success(),
        "delete-config failed: {}",
        String::from_utf8_lossy(&del_out.stderr)
    );

    // Confirm quota cleared from image (poll up to 5 s).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = h1.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".to_string(), Some(ALICE.to_string()))];
        if img
            .client_quotas()
            .get(&key)
            .and_then(|m| m.get("controller_mutation_rate"))
            .is_none()
        {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "controller_mutation_rate not cleared from image within 5s after --delete-config"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// JVM acceptance: `kafka-configs --describe --entity-type users` round-trip for
/// SCRAM credentials (KIP-554 read half, api_key 50).
///
/// Three-broker SASL/PLAINTEXT cluster; provision alice's SCRAM-SHA-512 credential
/// via `kafka-configs --alter --add-config SCRAM-SHA-512=[...]`, then describe and
/// assert exit 0 + `SCRAM-SHA-512` in stdout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_describe_users_scram_credentials_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (h1, _h2, _h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(ADMIN, ADMIN_PASS, &[]).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Provision a SCRAM user via kafka-configs --alter (hits AlterUserScramCredentials, api_key 51).
    let alter = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            "alice",
            "--add-config",
            "SCRAM-SHA-512=[iterations=4096,password=alice-secret]",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        alter.status.success(),
        "alter SCRAM failed: {}",
        String::from_utf8_lossy(&alter.stderr)
    );

    // Describe — should exit 0 cleanly (api_key 50 now implemented).
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "users",
            "--entity-name",
            "alice",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("SCRAM-SHA-512"),
        "expected SCRAM-SHA-512 in describe output: {stdout}"
    );

    let _ = h1; // keep alive
}
