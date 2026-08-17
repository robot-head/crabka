//! Differential test: produce a record through the gateway's `ProduceCore`
//! against a host-advertised in-process broker, then read it back with the JVM
//! `kafka-console-consumer` from a cp-kafka container. This test needs Docker.

use std::{
    collections::BTreeMap,
    io::Read,
    process::{Command, Output, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::{codec::RawCodec, produce::ProduceCore, types::GatewayRecord};
use crabka_security::ListenerProtocol;
use tokio::sync::oneshot;

const HOST_BOOTSTRAP: &str = "127.0.0.1:19092";
const DOCKER_BOOTSTRAP: &str = "host.docker.internal:19094";
const IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.5.0";
const TOPIC: &str = "gw-jvm";
const MARKER: &str = "jvm-sees-this";

// The JVM read is idempotent because it starts from the beginning of the log,
// so a failed `docker run` can run again. All attempts share one deadline, and
// each attempt is bounded by the time that remains, so a hung `docker` cannot
// hold the test until nextest kills it.
const CONSUMER_ATTEMPTS: usize = 3;
const CONSUMER_DEADLINE: Duration = Duration::from_secs(90);
const CONSUMER_RETRY_PAUSE: Duration = Duration::from_secs(2);
const CHILD_POLL: Duration = Duration::from_millis(100);
const DRAIN_GRACE: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_consumer_reads_gateway_output() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Give host-side Rust clients and the Dockerized JVM consumer distinct
    // listener identities. Metadata then advertises an address reachable from
    // the same network that made the request.
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listen_addr = HOST_BOOTSTRAP.parse().unwrap();
    config.advertised_listener = HOST_BOOTSTRAP.into();
    config.listeners = vec![
        ListenerSpec {
            name: "HOST".into(),
            bind_addr: HOST_BOOTSTRAP.parse().unwrap(),
            advertised: HOST_BOOTSTRAP.into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
        },
        ListenerSpec {
            name: "DOCKER".into(),
            bind_addr: "0.0.0.0:19094".parse().unwrap(),
            advertised: DOCKER_BOOTSTRAP.into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
        },
    ];
    config.inter_broker_listener_name = "HOST".into();
    let broker: BrokerHandle = Broker::start(config).await.expect("broker");

    let mut admin = AdminClient::connect(&[HOST_BOOTSTRAP.to_string()])
        .await
        .expect("admin");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: TOPIC.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            crabka_units::secs(10),
        )
        .await
        .expect("create");

    let core = ProduceCore::new(HOST_BOOTSTRAP, TOPIC, Arc::new(RawCodec), None)
        .await
        .expect("core");
    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    core.produce(
        GatewayRecord {
            topic: TOPIC.into(),
            key: None,
            value: Bytes::from_static(MARKER.as_bytes()),
            body_structured: None,
            headers: vec![],
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
        },
        &anon,
    )
    .await
    .expect("produce");
    // `ProduceCore::produce` uses `acks=all`, so the record is durable when the
    // call returns. The wait makes the visibility condition explicit and does
    // not sleep.
    broker.wait_until_high_watermark(TOPIC, 0, 1).await;

    let out = jvm_consume_with_retry().await;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert2::assert!(
        out.status.success(),
        "kafka-console-consumer failed: status={} stdout={stdout} stderr={stderr}",
        out.status,
    );
    assert2::assert!(
        stdout.contains(MARKER),
        "JVM consumer exited cleanly but did not print the record: stdout={stdout} stderr={stderr}",
    );

    broker.shutdown().await;
}

// Runs the JVM console consumer up to `CONSUMER_ATTEMPTS` times inside
// `CONSUMER_DEADLINE`.
//
// The exit status is checked before the stdout content. A `docker run` that
// fails before the consumer starts (no image, no daemon, a registry error)
// prints nothing on stdout, and a content-only assertion would hide the cause.
// A non-zero status is retried because such failures are usually transient.
// A clean exit is returned as is, so a consumer that connected and saw no
// record is reported by the caller as a broker defect, not retried.
async fn jvm_consume_with_retry() -> Output {
    let started = Instant::now();
    let mut attempt = 1;
    loop {
        let remaining = CONSUMER_DEADLINE.saturating_sub(started.elapsed());
        let out = jvm_consume_once(remaining).await;
        eprintln!(
            "CRABKA[jvm_differential] attempt={attempt}/{CONSUMER_ATTEMPTS} image={IMAGE} \
             status={} elapsed={:?} stderr={}",
            out.status,
            started.elapsed(),
            String::from_utf8_lossy(&out.stderr).trim(),
        );
        let out_of_budget = started.elapsed() + CONSUMER_RETRY_PAUSE >= CONSUMER_DEADLINE;
        if out.status.success() || attempt >= CONSUMER_ATTEMPTS || out_of_budget {
            return out;
        }
        attempt += 1;
        tokio::time::sleep(CONSUMER_RETRY_PAUSE).await;
    }
}

// Runs one `docker run` of the JVM console consumer and kills it when
// `deadline` passes.
//
// `std::process::Command::output` waits without a deadline, and this crate's
// tokio has no `process` feature. The child is polled from the async task, and
// the pipes drain on blocking threads so a full pipe cannot stall the child.
async fn jvm_consume_once(deadline: Duration) -> Output {
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            IMAGE,
            "kafka-console-consumer",
            "--bootstrap-server",
            DOCKER_BOOTSTRAP,
            "--topic",
            TOPIC,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "10000",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn docker run");
    let stdout = drain(child.stdout.take().expect("piped stdout"));
    let stderr = drain(child.stderr.take().expect("piped stderr"));
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll docker run") {
            break status;
        }
        if started.elapsed() >= deadline {
            // `kill` only fails when the child exited between the poll and
            // the signal. `wait` then reaps it either way.
            if let Err(err) = child.kill() {
                eprintln!("CRABKA[jvm_differential] kill docker run: {err}");
            }
            break child.wait().expect("reap docker run");
        }
        tokio::time::sleep(CHILD_POLL).await;
    };
    Output {
        status,
        stdout: join_drain(stdout).await,
        stderr: join_drain(stderr).await,
    }
}

// Reads a child pipe to its end on a plain thread. A read error ends the drain
// early. The exit status still shows what happened.
//
// The thread is not a `spawn_blocking` task on purpose. The runtime waits for
// blocking tasks when the test drops it, so a pipe that a stray grandchild
// keeps open after the kill would pin the test. A plain thread is left behind.
fn drain(mut pipe: impl Read + Send + 'static) -> oneshot::Receiver<Vec<u8>> {
    let (tx, rx) = oneshot::channel();
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Err(err) = pipe.read_to_end(&mut buf) {
            eprintln!("CRABKA[jvm_differential] read child pipe: {err}");
        }
        // The receiver is gone only when the drain grace passed. The bytes are
        // then no longer wanted.
        let _ = tx.send(buf);
    });
    rx
}

// A pipe closes when the child exits, unless a stray grandchild inherited it.
// The join gets a short bound so such a pipe cannot hold the test open after
// the kill.
async fn join_drain(rx: oneshot::Receiver<Vec<u8>>) -> Vec<u8> {
    match tokio::time::timeout(DRAIN_GRACE, rx).await {
        Ok(buf) => buf.expect("drain thread sends before it exits"),
        Err(_) => b"<pipe did not close after the child exited>".to_vec(),
    }
}
