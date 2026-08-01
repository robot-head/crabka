//! Round-trip a real JVM Kafka broker's log dir against `crabka-log`.
//!
//! Gated by `#[ignore]` so `cargo test` doesn't pull Docker by default.
//! Run with `--include-ignored` (or `--ignored`).
//!
//! The companion scenario `jvm_consumes_rust_written_log_dir` is currently
//! deferred — see `crates/log/tests/KNOWN_ISSUES.md`.

use std::{
    path::Path,
    process::{Command, Stdio},
};

use crabka_log::{Log, LogConfig};
use crabka_units::prelude::gibibytes;
use tempfile::tempdir;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::{KAFKA_PORT, Kafka};

const TOPIC: &str = "crabka-log-itest";

/// `docker exec <container_id> <args...>` — fails the test on non-zero exit.
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
