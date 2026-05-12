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
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(KAFKA_IMAGE)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn docker run");
    eprintln!(
        "CRABKA[test] docker_run {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run {args:?} failed: stdout={}, stderr={}",
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

    // Spawn brokers concurrently so each one's leader-wait sees its peers
    // come up. Sequential startup deadlocks: broker 1's `Broker::start`
    // blocks waiting for a leader, but a 3-voter quorum can't elect one
    // until brokers 2 and 3 are also up.
    let mut tempdirs = Vec::with_capacity(3);
    let mut spawns = Vec::with_capacity(3);
    for i in 0..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            // bind on 0.0.0.0 so Docker-side containers can reach us.
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
        };
        tempdirs.push(dir);
        spawns.push(tokio::spawn(async move {
            Broker::start(cfg).await.expect("broker start")
        }));
    }
    let mut cluster = Vec::with_capacity(3);
    for (spawn, dir) in spawns.into_iter().zip(tempdirs) {
        let handle = spawn.await.expect("spawn");
        cluster.push((handle, dir));
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

    // Parallel spawn — sequential startup deadlocks: broker 1's
    // `Broker::start` blocks waiting for a leader, but a 3-voter quorum
    // can't elect one until brokers 2 and 3 are also up.
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

    // 3. Produce 100 records via kafka-console-producer.
    let mut producer_child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            &bootstrap_1,
            "--topic",
            TOPIC,
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
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn transactional_console_producer_eos() {
    const TOPIC: &str = "crabka-txn-itest";

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
    //    `kafka-verifiable-producer` ships in cp-kafka 6.1.1 and supports
    //    `--transactional-id` / `--transaction-duration-ms` (added in Apache
    //    Kafka 0.11). It commits a new transaction at each duration interval.
    let producer_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
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
