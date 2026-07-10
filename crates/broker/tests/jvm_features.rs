//! KIP-1022 "updating features" — JVM acceptance.
//!
//! Drives the real `mirror.gcr.io/apache/kafka:4.0.0` `kafka-features` admin tool against an
//! in-process Crabka broker advertised at `host.docker.internal:9092`, proving
//! the `UpdateFeatures` / `ApiVersions` feature surface round-trips end to end:
//!
//! 1. `describe` lists Crabka's finalized features (`metadata.version`,
//!    `group.version`, `transaction.version`) at the self-bootstrap defaults.
//! 2. `downgrade --feature transaction.version=1` is accepted and a follow-up
//!    `describe` reflects the change.
//! 3. `upgrade --feature transaction.version=2` round-trips it back.
//!
//! Gated `#[ignore]` (requires Docker); run with `--ignored`. Binds host port
//! 9092, so it must not run concurrently with `jvm_acceptance` (single test
//! here keeps it self-contained).

use std::process::Command;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_log::LogConfig;

const BOOTSTRAP: &str = "host.docker.internal:9092";
const LISTEN: &str = "0.0.0.0:9092";
/// Kafka 4.0 is the first image whose `kafka-features` understands the
/// `group.version` / `transaction.version` feature surface (KIP-1022).
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";

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
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
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

/// Run `kafka-features <args>` from an `mirror.gcr.io/apache/kafka:4.0.0` container that can
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
    assert2::assert!(desc.status.success());
    let out = String::from_utf8_lossy(&desc.stdout);
    // (feature, pinned starting finalized level; None = only presence is pinned)
    let features = [
        ("metadata.version", None),
        ("group.version", Some(1)),
        ("transaction.version", Some(2)),
    ];
    for (feature, want_level) in features {
        assert2::assert!(out.contains(feature));
        if let Some(want) = want_level {
            assert2::assert!(finalized_level(&out, feature) == Some(want));
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
        assert2::assert!(out.status.success());
        let desc = kafka_features(&["describe"]);
        let text = String::from_utf8_lossy(&desc.stdout);
        assert2::assert!(finalized_level(&text, "transaction.version") == Some(want));
    }

    handle.shutdown().await;
}
