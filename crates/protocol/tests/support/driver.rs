//! Declarative battery of real JVM-client operations that, run against the
//! broker through the tap, emit a broad set of `(api_key, version)` pairs.
use std::process::Command;

/// Bootstrap the JVM CLI tools must dial so traffic traverses the tap.
/// From inside the container, the tap (on the host) is `host.docker.internal:TAP_PORT`.
pub const BOOTSTRAP: &str = "host.docker.internal:19091";

fn exec(container: &str, args: &[&str]) {
    let out = Command::new("docker")
        .arg("exec")
        .arg(container)
        .args(args)
        .output()
        .expect("docker exec");
    // Tools may legitimately fail (e.g. describe a missing group); we only
    // care about the wire traffic they emit en route. Log, don't assert.
    if !out.status.success() {
        eprintln!(
            "driver op {args:?} stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// Run the full battery. `mirror.gcr.io/apache/kafka:4.3.0` ships CLI tools under `/opt/kafka/bin`.
pub fn run(container: &str) {
    run_topic_ops(container);
    run_config_ops(container);
    run_message_ops(container);
    run_group_ops(container);
    run_admin_ops(container);
    run_admin_tail(container);
}

fn run_topic_ops(container: &str) {
    let bs = ["--bootstrap-server", BOOTSTRAP];
    let t = "/opt/kafka/bin";
    exec(
        container,
        &[
            &format!("{t}/kafka-topics.sh"),
            "--create",
            "--topic",
            "corpus-a",
            "--partitions",
            "3",
            "--replication-factor",
            "1",
            bs[0],
            bs[1],
        ],
    );
    exec(
        container,
        &[&format!("{t}/kafka-topics.sh"), "--list", bs[0], bs[1]],
    );
    exec(
        container,
        &[
            &format!("{t}/kafka-topics.sh"),
            "--describe",
            "--topic",
            "corpus-a",
            bs[0],
            bs[1],
        ],
    );
}

fn run_config_ops(container: &str) {
    let bs = ["--bootstrap-server", BOOTSTRAP];
    let t = "/opt/kafka/bin";
    exec(
        container,
        &[
            &format!("{t}/kafka-topics.sh"),
            "--alter",
            "--topic",
            "corpus-a",
            "--partitions",
            "5",
            bs[0],
            bs[1],
        ],
    );
}

fn run_message_ops(container: &str) {
    let bs = ["--bootstrap-server", BOOTSTRAP];
    let t = "/opt/kafka/bin";
    exec(
        container,
        &[
            &format!("{t}/kafka-configs.sh"),
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            "corpus-a",
            bs[0],
            bs[1],
        ],
    );
}

fn run_group_ops(container: &str) {
    let bs = ["--bootstrap-server", BOOTSTRAP];
    let t = "/opt/kafka/bin";
    exec(
        container,
        &[
            &format!("{t}/kafka-configs.sh"),
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            "corpus-a",
            "--add-config",
            "retention.ms=86400000",
            bs[0],
            bs[1],
        ],
    );
}

fn run_admin_ops(container: &str) {
    let bs = ["--bootstrap-server", BOOTSTRAP];
    let t = "/opt/kafka/bin";
    exec(
        container,
        &[
            &format!("{t}/kafka-configs.sh"),
            "--describe",
            "--entity-type",
            "brokers",
            "--entity-name",
            "1",
            bs[0],
            bs[1],
        ],
    );
    exec(
        container,
        &[
            "bash",
            "-lc",
            &format!(
                "echo 'k1:v1' | {t}/kafka-console-producer.sh --topic corpus-a --property parse.key=true --property key.separator=: --bootstrap-server {BOOTSTRAP}"
            ),
        ],
    );
    exec(
        container,
        &[
            "bash",
            "-lc",
            &format!(
                "timeout 5 {t}/kafka-console-consumer.sh --topic corpus-a --from-beginning --max-messages 1 --bootstrap-server {BOOTSTRAP} || true"
            ),
        ],
    );
    exec(
        container,
        &[
            &format!("{t}/kafka-get-offsets.sh"),
            "--topic",
            "corpus-a",
            bs[0],
            bs[1],
        ],
    );
    exec(
        container,
        &[
            "bash",
            "-lc",
            &format!(
                "timeout 5 {t}/kafka-console-consumer.sh --topic corpus-a --group cg1 --from-beginning --max-messages 1 --bootstrap-server {BOOTSTRAP} || true"
            ),
        ],
    );
    exec(
        container,
        &[
            &format!("{t}/kafka-consumer-groups.sh"),
            "--list",
            bs[0],
            bs[1],
        ],
    );
}

fn run_admin_tail(container: &str) {
    let bs = ["--bootstrap-server", BOOTSTRAP];
    let t = "/opt/kafka/bin";
    exec(
        container,
        &[
            &format!("{t}/kafka-consumer-groups.sh"),
            "--describe",
            "--group",
            "cg1",
            bs[0],
            bs[1],
        ],
    );
    exec(
        container,
        &[
            &format!("{t}/kafka-consumer-groups.sh"),
            "--describe",
            "--group",
            "cg1",
            "--offsets",
            bs[0],
            bs[1],
        ],
    );
    exec(
        container,
        &[
            &format!("{t}/kafka-acls.sh"),
            "--add",
            "--allow-principal",
            "User:alice",
            "--operation",
            "Read",
            "--topic",
            "corpus-a",
            bs[0],
            bs[1],
        ],
    );
    exec(
        container,
        &[&format!("{t}/kafka-acls.sh"), "--list", bs[0], bs[1]],
    );
    exec(
        container,
        &[
            &format!("{t}/kafka-leader-election.sh"),
            "--election-type",
            "preferred",
            "--all-topic-partitions",
            bs[0],
            bs[1],
        ],
    );
    exec(
        container,
        &[
            &format!("{t}/kafka-topics.sh"),
            "--delete",
            "--topic",
            "corpus-a",
            bs[0],
            bs[1],
        ],
    );
}
