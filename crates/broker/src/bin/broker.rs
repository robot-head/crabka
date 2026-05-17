//! `crabka-broker` — single-node Kafka-compatible broker daemon.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Parser;

use crabka_broker::{BootstrapMode, Broker, BrokerConfig};
use crabka_log::LogConfig;

#[derive(Debug, Parser)]
#[command(
    name = "crabka-broker",
    version,
    about = "Single-node Kafka-compatible broker (MVP)"
)]
struct Args {
    /// TCP address to listen on.
    #[arg(long, default_value = "127.0.0.1:9092")]
    listen_addr: SocketAddr,

    /// `host:port` to advertise to clients (defaults to `listen_addr`).
    /// Set via env `CRABKA_ADVERTISED_LISTENER` from the operator.
    #[arg(long, env = "CRABKA_ADVERTISED_LISTENER")]
    advertised_listener: Option<String>,

    /// Directory containing per-partition log dirs.
    #[arg(long, default_value = "./crabka-data")]
    log_dir: PathBuf,

    /// Numeric broker id.
    #[arg(long, default_value_t = 1)]
    broker_id: i32,

    /// Cluster UUID. Every broker in the same cluster must share this
    /// value. Set via env `CRABKA_CLUSTER_ID` from the operator
    /// (the `KafkaCluster` UID).
    #[arg(long, env = "CRABKA_CLUSTER_ID")]
    cluster_id: Option<uuid::Uuid>,

    /// Bind address for the Prometheus `/metrics` HTTP endpoint.
    /// Empty string (or `none`) disables. Defaults to `0.0.0.0:9404`
    /// — the same port `jmx_prometheus_javaagent` uses for vanilla
    /// Kafka, so existing scrape configs apply unchanged.
    #[arg(
        long,
        env = "CRABKA_METRICS_LISTEN_ADDR",
        default_value = "0.0.0.0:9404"
    )]
    metrics_listen_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_broker=info,crabka_log=info,info".into()),
        )
        .init();

    let args = Args::parse();
    let advertised = args
        .advertised_listener
        .unwrap_or_else(|| args.listen_addr.to_string());
    let controller_addr: std::net::SocketAddr = {
        let mut a = args.listen_addr;
        a.set_port(9093);
        a
    };
    let node_id = u64::try_from(args.broker_id).unwrap_or_else(|_| {
        eprintln!("broker_id must be non-negative");
        std::process::exit(1);
    });
    let metrics_listen_addr = parse_metrics_addr(&args.metrics_listen_addr)?;
    let bootstrap_mode = detect_bootstrap_mode(&args.log_dir);
    tracing::info!(?bootstrap_mode, log_dir = %args.log_dir.display(), "selected bootstrap mode");
    let config = BrokerConfig {
        broker_id: args.broker_id,
        listen_addr: args.listen_addr,
        advertised_listener: advertised,
        log_dir: args.log_dir,
        log_config: LogConfig::default(),
        node_id,
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(node_id, controller_addr)],
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: std::time::Duration::from_secs(5),
        controller_heartbeat_interval: std::time::Duration::from_millis(500),
        bootstrap_mode,
        cluster_id: args.cluster_id,
        metrics_listen_addr,
        ..BrokerConfig::default()
    };

    let handle = Broker::start(config).await?;
    tracing::info!(addr = %handle.listen_addr(), "crabka-broker listening");

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    handle.shutdown().await;
    tracing::info!("crabka-broker stopped");
    Ok(())
}

/// Map the `--metrics-listen-addr` CLI value onto an `Option<SocketAddr>`.
/// Empty string or `none` (case-insensitive) disables the endpoint;
/// anything else must parse as `SocketAddr`.
fn parse_metrics_addr(s: &str) -> Result<Option<SocketAddr>, Box<dyn std::error::Error>> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    Ok(Some(trimmed.parse()?))
}

/// Pick `Bootstrap` (fresh cluster) vs `Rejoin` (restart on existing
/// state) based on whether the raft log directory has been populated.
///
/// The broker hands `BrokerConfig.log_dir.join("__cluster_metadata")`
/// to `ControllerConfig.log_dir` (see `broker.rs:833`), and
/// `RaftLogStore::open` then puts its segment files under
/// `<that>/@metadata-0/`. So the absolute path of the raft segments is
/// `<log_dir>/__cluster_metadata/@metadata-0/`. On the first broker
/// boot the directory doesn't exist yet; on every subsequent boot it
/// has segment files from the prior run. Using directory presence +
/// non-empty as the signal matches `Controller::start`'s
/// `log_is_empty` check without having to open the log store from
/// here.
fn detect_bootstrap_mode(log_dir: &Path) -> BootstrapMode {
    let raft_log_dir = log_dir.join("__cluster_metadata").join("@metadata-0");
    let has_state = match std::fs::read_dir(&raft_log_dir) {
        Ok(mut entries) => entries.next().is_some(),
        Err(_) => false,
    };
    if has_state {
        BootstrapMode::Rejoin
    } else {
        BootstrapMode::Bootstrap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn detect_bootstrap_when_log_dir_is_empty() {
        let dir = tempdir().unwrap();
        assert_eq!(detect_bootstrap_mode(dir.path()), BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_metadata_dir_missing() {
        let dir = tempdir().unwrap();
        // log_dir exists with unrelated content (bootstrap.json from
        // `crabka format`) but no __cluster_metadata/@metadata-0 subdir.
        std::fs::write(dir.path().join("bootstrap.json"), "{}").unwrap();
        assert_eq!(detect_bootstrap_mode(dir.path()), BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_rejoin_when_metadata_dir_has_state() {
        let dir = tempdir().unwrap();
        let meta = dir.path().join("__cluster_metadata").join("@metadata-0");
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(meta.join("00000000000000000000.log"), b"segment").unwrap();
        assert_eq!(detect_bootstrap_mode(dir.path()), BootstrapMode::Rejoin);
    }

    #[test]
    fn detect_bootstrap_when_metadata_dir_empty() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("__cluster_metadata").join("@metadata-0"))
            .unwrap();
        // empty @metadata-0 dir is treated as no state (corner case:
        // crashed first start before any segment was written).
        assert_eq!(detect_bootstrap_mode(dir.path()), BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_only_outer_cluster_metadata_dir_exists() {
        let dir = tempdir().unwrap();
        // The outer __cluster_metadata dir exists but the inner
        // @metadata-0 subdir doesn't — should still be Bootstrap.
        std::fs::create_dir_all(dir.path().join("__cluster_metadata")).unwrap();
        assert_eq!(detect_bootstrap_mode(dir.path()), BootstrapMode::Bootstrap);
    }
}
