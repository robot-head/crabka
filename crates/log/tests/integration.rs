//! Round-trip a real JVM Kafka broker's log dir against `crabka-log`.
//!
//! These tests carry `#[ignore]`, so `cargo test` does not pull Docker by
//! default. Run them with `--include-ignored` or `--ignored`.
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use crabka_log::{Log, LogConfig};
use crabka_protocol::records::{Record, RecordBatch};
use crabka_units::prelude::gibibytes;
use tempfile::tempdir;
use testcontainers::{ImageExt, core::Mount, runners::AsyncRunner};
use testcontainers_modules::kafka::{KAFKA_PORT, Kafka};

const TOPIC: &str = "crabka-log-itest";

/// `docker exec <container_id> <args...>`. This fails the test on a non-zero
/// exit.
fn docker_exec(container_id: &str, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new("docker");
    cmd.arg("exec").arg(container_id).args(args);
    let out = cmd
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("spawn docker exec");
    assert2::assert!(out.status.success());
    out
}

/// `docker exec -i <container_id> <args...>` and pipe `stdin` into it.
fn docker_exec_stdin(container_id: &str, args: &[&str], stdin: &[u8]) {
    use std::io::Write;
    let mut child = Command::new("docker")
        .arg("exec")
        .arg("-i")
        .arg(container_id)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn docker exec");
    child
        .stdin
        .as_mut()
        .expect("stdin piped")
        .write_all(stdin)
        .expect("write stdin");
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait docker exec");
    assert2::assert!(out.status.success());
}

/// `docker cp <container_id>:<src> <dst>`.
fn docker_cp(container_id: &str, src: &str, dst: &Path) {
    let out = Command::new("docker")
        .arg("cp")
        .arg(format!("{container_id}:{src}"))
        .arg(dst)
        .output()
        .expect("spawn docker cp");
    assert2::assert!(out.status.success());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn read_jvm_produced_log_dir() {
    let kafka = Kafka::default()
        .start()
        .await
        .expect("start kafka container");
    let container_id = kafka.id().to_string();

    // testcontainers-modules' Confluent Kafka module advertises
    //   PLAINTEXT://localhost:<host-mapped-port>,BROKER://localhost:9092
    // The PLAINTEXT listener (KAFKA_PORT = 9093 inside the container) is
    // advertised on the host-mapped port — that address is unreachable
    // from inside the container. The BROKER listener at localhost:9092 is
    // advertised with an in-container-resolvable address, so for
    // `docker exec`-ed clients we must use BROKER's 9092, not KAFKA_PORT.
    let _host_port = kafka
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("get host port");
    let bootstrap = "localhost:9092";

    // 1. Create the topic.
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
            bootstrap,
        ],
    );

    // 2. Produce a handful of keyed records.
    let stdin = b"k1:v1\nk2:v2\nk3:v3\n";
    docker_exec_stdin(
        &container_id,
        &[
            "kafka-console-producer",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            TOPIC,
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=:",
        ],
        stdin,
    );

    // 3. Locate the partition dir inside the container. The confluent
    //    image uses `/var/lib/kafka/data` as the log dir; the partition
    //    dir is named `<topic>-<partition>`.
    let partition_dir = format!("/var/lib/kafka/data/{TOPIC}-0");
    // Sanity: list the directory so we get a useful error if it's missing.
    docker_exec(&container_id, &["ls", "-la", &partition_dir]);

    // 4. Copy the partition dir out of the container.
    let host_tmp = tempdir().expect("tempdir");
    let host_target = host_tmp.path().join(format!("{TOPIC}-0"));
    docker_cp(&container_id, &partition_dir, host_tmp.path());
    assert2::assert!(host_target.exists());

    // 5. Open with crabka-log and read everything back.
    let log = Log::open(&host_target, LogConfig::default()).expect("open log");
    let out = log
        .read(log.log_start_offset(), gibibytes(4))
        .expect("read log");
    assert2::assert!(!out.batches.is_empty());
    let total_records: usize = out.batches.iter().map(|b| b.records.len()).sum();
    assert2::assert!(total_records >= 3);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn jvm_consumes_rust_written_log_dir() {
    let host_tmp = tempdir().expect("tempdir");
    let host_data = host_tmp
        .path()
        .to_str()
        .expect("temporary path must be UTF-8");
    let kafka = Kafka::default()
        // The broker and the test process have different host UIDs. Root is
        // limited to this disposable container and can reopen both sets of
        // files after the restart.
        .with_user("root")
        .with_mount(Mount::bind_mount(host_data, "/var/lib/kafka/data"))
        .start()
        .await
        .expect("start kafka container");
    let container_id = kafka.id().to_string();
    let bootstrap = "localhost:9092";

    docker_exec(
        &container_id,
        &[
            "kafka-topics",
            "--create",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            bootstrap,
        ],
    );
    docker_exec(
        &container_id,
        &["chmod", "-R", "a+rwX", "/var/lib/kafka/data"],
    );
    kafka
        .stop_with_timeout(Some(30))
        .await
        .expect("stop kafka container");

    let timestamp = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_millis(),
    )
    .expect("timestamp must fit i64");
    let mut batch = RecordBatch {
        base_timestamp: timestamp,
        max_timestamp: timestamp + 2,
        last_offset_delta: 2,
        records: (0..3)
            .map(|offset_delta| Record {
                timestamp_delta: i64::from(offset_delta),
                offset_delta,
                key: Some(Bytes::from(format!("k{}", offset_delta + 1))),
                value: Some(Bytes::from(format!("v{}", offset_delta + 1))),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    };
    let partition_dir = host_tmp.path().join(format!("{TOPIC}-0"));
    let mut log = Log::open(
        partition_dir,
        LogConfig {
            flush_on_append: true,
            ..LogConfig::default()
        },
    )
    .expect("open JVM partition with crabka-log");
    log.append(&mut batch).expect("append Rust-written batch");
    drop(log);

    kafka.start().await.expect("restart kafka container");
    let out = docker_exec(
        &container_id,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "20000",
            "--property",
            "print.key=true",
            "--property",
            "key.separator=:",
        ],
    );
    assert2::assert!(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .collect::<Vec<_>>()
            == ["k1:v1", "k2:v2", "k3:v3"]
    );
}
