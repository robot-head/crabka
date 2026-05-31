//! THROWAWAY KIP-595 slice-0 acceptance test (requires Docker + the
//! `kraft-spike` feature):
//!
//! ```text
//! cargo test -p crabka-broker --features kraft-spike --test kraft_spike_jvm \
//!   -- --ignored --nocapture
//! ```
//!
//! Boots a single-node Crabka controller (serving real KRaft `ApiVersions` +
//! `Fetch` on its controller listener via the `kraft-spike` hook) and a live
//! `apache/kafka:4.0.0` broker observer pointed at it. The observer's RaftClient
//! `Fetch`es `__cluster_metadata-0`; the spike replays the captured JVM
//! bootstrap log (offsets 0..=5) back to it. Success = the observer decodes the
//! log with no CRC/format errors and advances its fetch offset to the high
//! watermark (6), which the Crabka controller logs as it serves the Fetch.

use std::process::Command;
use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_log::LogConfig;

const KAFKA_IMAGE: &str = "apache/kafka:4.0.0";
const OBS_NAME: &str = "crabka-kraft-spike-obs";

async fn start_spike_controller() -> (BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_raft=info,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let controller_addr: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let config = BrokerConfig {
        broker_id: 1,
        node_id: 1,
        listen_addr: "0.0.0.0:9092".parse().expect("static addr"),
        advertised_listener: "host.docker.internal:9092".into(),
        log_dir: dir.path().to_path_buf(),
        log_config: LogConfig::default(),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(1, controller_addr)],
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start spike controller");
    eprintln!("CRABKA[test] spike controller started, controller listener on 0.0.0.0:9093");
    (handle, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + kraft-spike feature"]
async fn jvm_observer_fetches_metadata() {
    let (controller, _dir) = start_spike_controller().await;
    let _ = Command::new("docker").args(["rm", "-f", OBS_NAME]).output();

    // The cluster id is irrelevant to decode-only: the spike never rejects on
    // INCONSISTENT_CLUSTER_ID, so the observer's configured id need not match.
    let cluster_id = "EZhlvZa_SRy78NRDuVm4Qw";
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            OBS_NAME,
            "--add-host=host.docker.internal:host-gateway",
            "-e",
            "KAFKA_NODE_ID=2",
            "-e",
            "KAFKA_PROCESS_ROLES=broker",
            "-e",
            "KAFKA_LISTENERS=PLAINTEXT://:9092",
            "-e",
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://host.docker.internal:19092",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@host.docker.internal:9093",
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "-e",
            &format!("CLUSTER_ID={cluster_id}"),
            KAFKA_IMAGE,
        ])
        .status()
        .expect("docker run observer");
    assert!(status.success(), "failed to start JVM observer container");

    // Give the observer time to negotiate ApiVersions and Fetch the log.
    tokio::time::sleep(Duration::from_secs(25)).await;

    let logs = Command::new("docker")
        .args(["logs", OBS_NAME])
        .output()
        .expect("docker logs");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
    let _ = Command::new("docker").args(["rm", "-f", OBS_NAME]).output();
    controller.shutdown().await;

    // Dump the observer log so the iteration loop can read the real signals.
    eprintln!("================ JVM OBSERVER LOG ================");
    eprintln!("{text}");
    eprintln!("================ END OBSERVER LOG ================");

    // No record-format/decode error on the metadata log. (BROKER_REGISTRATION
    // UnsupportedVersionException IS expected and out of scope — the observer
    // tries to register as a broker over a separate management RPC the spike
    // does not serve; that is not a metadata-log decode error.)
    for needle in [
        "CorruptRecordException",
        "InvalidRecordException",
        "Error while reading the metadata log",
        "Encountered fatal fault",
    ] {
        assert!(
            !text.contains(needle),
            "JVM observer reported a record/decode error ({needle})"
        );
    }

    // Positive proof the observer Fetched + DECODED the served log: it caught up
    // to the high watermark (6), generated a snapshot at the last offset (5),
    // and parsed our FeatureLevelRecord into metadata.version 4.0-IV3 (level 25).
    assert!(
        text.contains("finished catching up to the current high water mark of 6"),
        "observer did not catch up to hwm=6 — it did not decode the served log"
    );
    assert!(
        text.contains("Publishing initial metadata at offset OffsetAndEpoch(offset=5, epoch=1)"),
        "observer did not publish a metadata image built from the served log"
    );
    assert!(
        text.contains("metadata.version Optional[4.0-IV3]"),
        "observer did not parse the FeatureLevelRecord (metadata.version=25 / 4.0-IV3)"
    );
}
