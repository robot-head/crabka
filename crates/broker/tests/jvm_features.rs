//! KIP-1022 "updating features" — JVM acceptance.
//!
//! Drives the real `mirror.gcr.io/apache/kafka:4.3.1` `kafka-features` and
//! `kafka-metadata-quorum` admin tools against an
//! in-process Crabka broker advertised at `host.docker.internal:9092`, proving
//! the `UpdateFeatures` / `ApiVersions` feature surface round-trips end to end:
//!
//! 1. `describe` lists Crabka's finalized features (`metadata.version`,
//!    `group.version`, `transaction.version`) at the self-bootstrap defaults.
//! 2. `downgrade --feature transaction.version=1` is accepted and a follow-up
//!    `describe` reflects the change.
//! 3. `upgrade --feature transaction.version=2` round-trips it back.
//! 4. `kraft.version` upgrades from 0 to 1, survives `describe`, and cannot be
//!    downgraded.
//! 5. `kafka-metadata-quorum describe` reads the exact live voter identity and
//!    `remove-controller` reaches the last-voter safety check.
//!
//! Gated `#[ignore]` (requires Docker); run with `--ignored`. Binds host port
//! 9092, so it must not run concurrently with `jvm_acceptance` (single test
//! here keeps it self-contained).

use std::process::Command;

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_log::LogConfig;

const BOOTSTRAP: &str = "host.docker.internal:9092";
const LISTEN: &str = "0.0.0.0:9092";
/// Kafka 4.3.1 is the compatibility oracle for KIP-853 and KIP-1186.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";
const CONTROLLER_BOOTSTRAP: &str = "host.docker.internal:9093";
const DIRECTORY_ID: uuid::Uuid = uuid::Uuid::from_u128(1);
const DIRECTORY_ID_BASE64: &str = "AAAAAAAAAAAAAAAAAAAAAQ";
const JOINER_CONTROLLER: &str = "host.docker.internal:9094";
const JOINER_DIRECTORY_ID: uuid::Uuid = uuid::Uuid::from_u128(2);
const JOINER_DIRECTORY_ID_BASE64: &str = "AAAAAAAAAAAAAAAAAAAAAg";

/// Boot an in-process Crabka broker listening on `LISTEN`, advertised as
/// `host.docker.internal:9092`. A standalone self-bootstrap finalizes the
/// latest-release feature defaults (metadata.version=25, group.version=1,
/// transaction.version=2).
async fn start_host_broker() -> (BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info,warn")),
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
        node_id: crabka_broker::NodeId(1),
        directory_id: DIRECTORY_ID,
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(
            crabka_broker::NodeId(1),
            CONTROLLER_BOOTSTRAP.to_string(),
        )],
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!("CRABKA[test] broker started listen={LISTEN} advertised={BOOTSTRAP}");
    (handle, dir)
}

/// Start a caught-up controller observer without auto-join. The official JVM
/// `add-controller` command promotes this exact live identity.
async fn start_host_observer() -> (crabka_raft::ControllerHandle, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = crabka_raft::ControllerConfig::for_tests(
        crabka_raft::NodeId(2),
        dir.path().join("__cluster_metadata"),
    );
    config.directory_id = JOINER_DIRECTORY_ID;
    config.controller_listen_addr = "0.0.0.0:9094".parse().expect("static addr");
    config.bootstrap_mode = crabka_raft::BootstrapMode::Join;
    config.cluster_id = Some(uuid::Uuid::nil());
    config.initial_voters = crabka_metadata::VoterSet::from_voters([crabka_metadata::Voter {
        id: crabka_raft::NodeId(1),
        directory_id: DIRECTORY_ID,
        endpoints: vec![crabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: "127.0.0.1".into(),
            port: 9093,
        }],
        kraft_version: crabka_metadata::KRaftVersionRange { min: 0, max: 1 },
    }]);
    let handle = crabka_raft::Controller::start(config)
        .await
        .expect("start observer");
    (handle, dir)
}

/// Run `kafka-features <args>` from the Kafka oracle container that can
/// reach the host broker via `--add-host=host.docker.internal:host-gateway`.
fn kafka_features(args: &[&str]) -> std::process::Output {
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-features.sh",
        "--bootstrap-server",
        BOOTSTRAP,
    ];
    full.extend_from_slice(args);
    let out = Command::new("docker")
        .args(&full)
        .output()
        .expect("spawn docker run kafka-features");
    eprintln!(
        "CRABKA[test] kafka-features {args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// Run `kafka-metadata-quorum <args>` against the controller listener.
fn kafka_metadata_quorum(args: &[&str]) -> std::process::Output {
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        "/opt/kafka/bin/kafka-metadata-quorum.sh",
        "--bootstrap-controller",
        CONTROLLER_BOOTSTRAP,
    ];
    full.extend_from_slice(args);
    let output = Command::new("docker")
        .args(&full)
        .output()
        .expect("spawn docker run kafka-metadata-quorum");
    eprintln!(
        "CRABKA[test] kafka-metadata-quorum {args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

/// Run Kafka 4.3.1's `add-controller` with a real controller properties file
/// and Kafka-compatible `meta.properties` for node 2.
fn kafka_add_controller() -> std::process::Output {
    let mount_dir = tempfile::tempdir().expect("controller command config dir");
    let metadata_dir = mount_dir.path().join("metadata");
    std::fs::create_dir(&metadata_dir).expect("create metadata dir");
    let properties = format!(
        "process.roles=controller\n\
         node.id=2\n\
         controller.listener.names=CONTROLLER\n\
         listeners=CONTROLLER://{JOINER_CONTROLLER}\n\
         listener.security.protocol.map=CONTROLLER:PLAINTEXT\n\
         controller.quorum.bootstrap.servers={CONTROLLER_BOOTSTRAP}\n\
         log.dirs=/tmp/kraft-controller-2\n"
    );
    let properties_path = mount_dir.path().join("controller.properties");
    std::fs::write(&properties_path, properties).expect("write controller properties");
    std::fs::write(
        metadata_dir.join("meta.properties"),
        format!(
            "version=1\ncluster.id=AAAAAAAAAAAAAAAAAAAAAA\nnode.id=2\ndirectory.id={JOINER_DIRECTORY_ID_BASE64}\n"
        ),
    )
    .expect("write meta.properties");
    let properties_mount = format!(
        "{}:/tmp/controller.properties:ro",
        properties_path.display()
    );
    let metadata_mount = format!("{}:/tmp/kraft-controller-2", metadata_dir.display());
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &properties_mount,
            "-v",
            &metadata_mount,
            KAFKA_IMAGE,
            "/opt/kafka/bin/kafka-metadata-quorum.sh",
            "--bootstrap-controller",
            CONTROLLER_BOOTSTRAP,
            "--command-config",
            "/tmp/controller.properties",
            "add-controller",
        ])
        .output()
        .expect("spawn docker run kafka-metadata-quorum add-controller");
    eprintln!(
        "CRABKA[test] kafka-metadata-quorum add-controller status={}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

/// Extract `FinalizedVersionLevel` for `feature` from `kafka-features describe`
/// output. Returns `None` if the feature is absent or shows no finalized level.
fn finalized_level(describe_stdout: &str, feature: &str) -> Option<i64> {
    for line in describe_stdout.lines() {
        if line.contains(&format!("Feature: {feature}")) {
            let idx = line.find("FinalizedVersionLevel:")?;
            let rest = &line[idx + "FinalizedVersionLevel:".len()..];
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_features_describe_and_round_trip() {
    let (handle, _dir) = start_host_broker().await;

    // 1. describe — Crabka advertises + finalizes the feature surface.
    let desc = kafka_features(&["describe"]);
    assert!(desc.status.success(), "describe failed");
    let out = String::from_utf8_lossy(&desc.stdout);
    // (feature, pinned starting finalized level; None = only presence is pinned)
    let features = [
        ("metadata.version", None),
        ("group.version", Some(1)),
        ("transaction.version", Some(2)),
    ];
    for (feature, want_level) in features {
        assert!(
            out.contains(feature),
            "describe must list {feature}:\n{out}"
        );
        if let Some(want) = want_level {
            assert!(
                finalized_level(&out, feature) == Some(want),
                "{feature} must start finalized at {want}:\n{out}"
            );
        }
    }

    // 2. downgrade transaction.version 2 -> 1 (within the advertised range),
    // 3. then upgrade it 1 -> 2 again; a follow-up describe reflects each change.
    let round_trip = [
        ("downgrade", "transaction.version=1", 1),
        ("upgrade", "transaction.version=2", 2),
    ];
    for (verb, spec, want) in round_trip {
        let out = kafka_features(&[verb, "--feature", spec]);
        assert!(
            out.status.success(),
            "{verb} {spec} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let desc = kafka_features(&["describe"]);
        let text = String::from_utf8_lossy(&desc.stdout);
        assert!(
            finalized_level(&text, "transaction.version") == Some(want),
            "transaction.version should be {want} after {verb}:\n{text}"
        );
    }

    // 4. KIP-853 activation is a Raft control operation, not an ordinary
    // FeatureLevelRecord. Kafka 4.3.1 must observe the finalized level and the
    // irreversible level-one boundary.
    let upgrade = kafka_features(&["upgrade", "--feature", "kraft.version=1"]);
    assert!(
        upgrade.status.success(),
        "kraft.version upgrade failed: {}",
        String::from_utf8_lossy(&upgrade.stderr)
    );
    let desc = kafka_features(&["describe"]);
    let text = String::from_utf8_lossy(&desc.stdout);
    assert!(
        finalized_level(&text, "kraft.version") == Some(1),
        "kraft.version should be finalized at 1:\n{text}"
    );
    let downgrade = kafka_features(&["downgrade", "--feature", "kraft.version=0"]);
    assert!(
        !downgrade.status.success(),
        "kraft.version downgrade must fail"
    );

    // 5. Exercise the official quorum tool on the controller listener. A live
    // observer is promoted by `add-controller`, then removed by
    // `remove-controller` before the last-voter rejection is checked.
    let (observer, _observer_dir) = start_host_observer().await;
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        observer
            .watch_image()
            .wait_for(|image| image.kraft_version() == 1),
    )
    .await
    .expect("observer did not fetch kraft.version=1 within 30s")
    .expect("observer image channel closed");
    let add = kafka_add_controller();
    assert!(
        add.status.success(),
        "add-controller failed: {}",
        String::from_utf8_lossy(&add.stderr)
    );
    handle
        .wait_for_image(|image| image.voters().len() == 2)
        .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        observer
            .watch_image()
            .wait_for(|image| image.voters().len() == 2),
    )
    .await
    .expect("observer did not commit the second voter within 30s")
    .expect("observer image channel closed");

    let quorum = kafka_metadata_quorum(&["describe", "--status"]);
    assert!(
        quorum.status.success(),
        "metadata quorum describe failed: {}",
        String::from_utf8_lossy(&quorum.stderr)
    );
    let quorum_text = String::from_utf8_lossy(&quorum.stdout);
    assert!(
        quorum_text
            .lines()
            .any(|line| line.starts_with("LeaderId:") && line.ends_with('1'))
    );
    assert!(
        quorum_text.lines().any(|line| {
            line.starts_with("CurrentVoters:")
                && line.contains("\"id\": 1")
                && line.contains("CONTROLLER://host.docker.internal:9093")
                && line.contains("\"id\": 2")
                && line.contains(JOINER_CONTROLLER)
        }),
        "unexpected voter projection:\n{quorum_text}"
    );

    let remove_joiner = kafka_metadata_quorum(&[
        "remove-controller",
        "--controller-id",
        "2",
        "--controller-directory-id",
        JOINER_DIRECTORY_ID_BASE64,
    ]);
    assert!(
        remove_joiner.status.success(),
        "remove-controller failed: {}",
        String::from_utf8_lossy(&remove_joiner.stderr)
    );
    handle
        .wait_for_image(|image| image.voters().len() == 1)
        .await;

    let remove = kafka_metadata_quorum(&[
        "remove-controller",
        "--controller-id",
        "1",
        "--controller-directory-id",
        DIRECTORY_ID_BASE64,
    ]);
    assert!(
        !remove.status.success(),
        "removing the last voter must fail"
    );
    let remove_error = String::from_utf8_lossy(&remove.stderr);
    assert!(
        remove_error.contains("last voter") || remove_error.contains("INVALID_REQUEST"),
        "unexpected remove-controller error: {remove_error}"
    );

    observer.shutdown().await;
    handle.shutdown().await;
}
