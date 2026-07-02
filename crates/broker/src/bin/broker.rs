//! `crabka-broker` — single-node Kafka-compatible broker daemon.

// Heap profiling: install jemalloc as the global allocator and enable the
// prof:true malloc_conf so jemalloc_pprof can dump heap profiles at runtime.
// Gated on both `unix` (tikv-jemallocator is Unix-only) and the
// `heap-profiling` feature so `cargo build` on Windows compiles cleanly.
#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use clap::Parser;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig};
use crabka_log::LogConfig;

/// Parse `--process-roles` string values into `NodeRole`s.
fn parse_roles_arg(roles: &[String]) -> Result<Vec<crabka_broker::config::NodeRole>, String> {
    use crabka_broker::config::NodeRole;
    roles
        .iter()
        .map(|r| match r.to_ascii_lowercase().as_str() {
            "controller" => Ok(NodeRole::Controller),
            "broker" => Ok(NodeRole::Broker),
            other => Err(format!(
                "unknown --process-roles value `{other}` (expected `controller` or `broker`)"
            )),
        })
        .collect()
}

#[derive(Debug, Parser)]
#[command(
    name = "crabka-broker",
    version,
    about = "Single-node Kafka-compatible broker (MVP)"
)]
struct Args {
    /// TCP address to listen on. Mutually exclusive with `--config-file`.
    #[arg(long, default_value = "127.0.0.1:9092", conflicts_with = "config_file")]
    listen_addr: SocketAddr,

    /// `host:port` to advertise to clients (defaults to `listen_addr`).
    /// Set via env `CRABKA_ADVERTISED_LISTENER` from the operator.
    /// Mutually exclusive with `--config-file`.
    #[arg(
        long,
        env = "CRABKA_ADVERTISED_LISTENER",
        conflicts_with = "config_file"
    )]
    advertised_listener: Option<String>,

    /// Path to a TOML config file (operator-managed). When set,
    /// `--listen-addr` / `--advertised-listener` must NOT be set;
    /// listener configuration comes from the file's `[[listeners]]`
    /// table. See `crabka_broker::file_config::FileConfig`.
    #[arg(long)]
    config_file: Option<PathBuf>,

    /// Primary log directory. Holds the cluster-metadata raft log and is
    /// the default partition data directory.
    #[arg(long, default_value = "./crabka-data")]
    log_dir: PathBuf,

    /// Additional JBOD data directories (KIP-113), comma-separated. New
    /// partitions are spread across `--log-dir` plus these by least-loaded
    /// placement. The cluster-metadata log always stays on `--log-dir`.
    /// Maps to Kafka's `log.dirs` having more than one entry.
    #[arg(
        long,
        env = "CRABKA_EXTRA_LOG_DIRS",
        value_delimiter = ',',
        num_args = 0..
    )]
    extra_log_dirs: Vec<PathBuf>,

    /// Numeric broker id.
    #[arg(long, default_value_t = 1)]
    broker_id: i32,

    /// `KRaft` `process.roles`, comma-separated (`controller`, `broker`).
    /// Defaults to the combined set when unset. The operator normally sets
    /// this via the `[process]` section of `--config-file` instead.
    #[arg(
        long,
        env = "CRABKA_PROCESS_ROLES",
        value_delimiter = ',',
        num_args = 0..
    )]
    process_roles: Vec<String>,

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

    /// Partition disk-usage scan cadence, in seconds. `0`
    /// disables the scanner entirely. The rebalancer's usage scraper
    /// reads the `partition_disk_bytes` gauge this populates.
    #[arg(
        long,
        env = "CRABKA_PARTITION_DISK_SCAN_INTERVAL_SECS",
        default_value_t = 60
    )]
    partition_disk_scan_interval_secs: u64,

    /// KIP-853: controller endpoints to discover the quorum leader at cold
    /// start, comma-separated `host:port`. Used by joiner nodes (those
    /// formatted without `--standalone` / `--initial-controllers`). Maps to
    /// Kafka's `controller.quorum.bootstrap.servers`.
    #[arg(
        long,
        env = "CRABKA_CONTROLLER_BOOTSTRAP_SERVERS",
        value_delimiter = ',',
        num_args = 0..
    )]
    controller_bootstrap_servers: Vec<SocketAddr>,

    /// KIP-853: auto-join the quorum as a voter once caught up as an
    /// observer. Maps to Kafka's `controller.quorum.auto.join.enable`.
    #[arg(long, env = "CRABKA_CONTROLLER_AUTO_JOIN")]
    controller_auto_join: bool,
}

#[tokio::main]
#[allow(clippy::too_many_lines)] // binary entrypoint: linear startup wiring
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Install the tracing subscriber — stdout `fmt` plus an
    // optional OTLP export layer. OTLP stays off unless the environment
    // opts in (see `crabka_broker::telemetry`). Built here, inside the
    // tokio runtime, so the gRPC exporter captures the runtime handle.
    let otlp = crabka_broker::telemetry::OtlpConfig::from_env(
        |k| std::env::var(k).ok(),
        &args.broker_id.to_string(),
        env!("CARGO_PKG_VERSION"),
        "crabka-broker",
    );
    let telemetry = crabka_broker::telemetry::init(
        otlp,
        "crabka_broker=info,crabka_log=info,info",
        "info,crabka_broker::request=debug,crabka_log=info",
        "crabka-broker",
    )?;
    let file_config: Option<crabka_broker::file_config::FileConfig> =
        match args.config_file.as_ref() {
            Some(p) => {
                let contents = std::fs::read_to_string(p)
                    .map_err(|e| format!("failed to read {}: {e}", p.display()))?;
                Some(
                    toml::from_str(&contents)
                        .map_err(|e| format!("failed to parse {}: {e}", p.display()))?,
                )
            }
            None => None,
        };
    let advertised = args
        .advertised_listener
        .unwrap_or_else(|| args.listen_addr.to_string());
    let controller_addr: std::net::SocketAddr = {
        let mut a = args.listen_addr;
        a.set_port(9093);
        // Under `--config-file` (operator/StatefulSet mode), `--listen-addr`
        // conflicts_with the config file, so `args.listen_addr` keeps its
        // 127.0.0.1:9092 default. Peers dial this broker's controller via its
        // pod FQDN, so binding the controller listener to loopback would make
        // it unreachable across pods — bind all interfaces (0.0.0.0) instead.
        if args.config_file.is_some() {
            a.set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
        }
        a
    };
    let node_id = u64::try_from(args.broker_id).unwrap_or_else(|_| {
        eprintln!("broker_id must be non-negative");
        std::process::exit(1);
    });
    let metrics_listen_addr = parse_metrics_addr(&args.metrics_listen_addr)?;
    let mut config = BrokerConfig {
        broker_id: args.broker_id,
        listen_addr: args.listen_addr,
        advertised_listener: advertised,
        log_dir: args.log_dir,
        extra_log_dirs: args.extra_log_dirs,
        log_config: LogConfig::default(),
        node_id,
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(node_id, controller_addr.to_string())],
        bootstrap_servers: args.controller_bootstrap_servers,
        // Placeholder — replaced from `meta.properties.json` (written by
        // `crabka format`) once `log_dir` is resolved against the TOML.
        directory_id: uuid::Uuid::nil(),
        auto_join: args.controller_auto_join,
        observer_lag_bound: 100,
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: std::time::Duration::from_secs(5),
        controller_heartbeat_interval: std::time::Duration::from_millis(500),
        // Placeholder — overwritten after `apply_to` against the final `log_dir`.
        bootstrap_mode: BootstrapMode::Bootstrap,
        cluster_id: args.cluster_id,
        metrics_listen_addr,
        partition_disk_scan_interval_secs: args.partition_disk_scan_interval_secs,
        ..BrokerConfig::default()
    };
    if !args.process_roles.is_empty() {
        config.roles = parse_roles_arg(&args.process_roles)?;
    }
    if let Some(fc) = file_config {
        fc.apply_to(&mut config)?;
    }
    // Detect against the *resolved* log_dir so a TOML override picks up
    // its on-disk state rather than the CLI-default empty path. This is
    // the difference between a fresh-pod Bootstrap and a rolled-pod
    // Rejoin against an existing PVC.
    config.bootstrap_mode = detect_bootstrap_mode(&config.log_dir);
    // KIP-853: recover this replica's stable directory id, written by
    // `crabka format`. Required for every formatted node; absence means the
    // dir was never formatted, which is an operator error.
    config.directory_id = crabka_broker::bootstrap::read_directory_id(&config.log_dir)?;
    tracing::info!(
        bootstrap_mode = ?config.bootstrap_mode,
        directory_id = %config.directory_id,
        log_dir = %config.log_dir.display(),
        "selected bootstrap mode"
    );

    let handle = Broker::start(config).await?;
    tracing::info!(addr = %handle.listen_addr(), "crabka-broker listening");

    let mut shutdown_rx = handle.should_shutdown_rx();
    tokio::select! {
        signal = wait_for_termination_signal() => {
            tracing::info!(signal, "shutdown signal received");
        }
        () = async {
            // Wait until the self-shutdown flag flips true.
            loop {
                // Check first in case the flag was already set before we subscribed.
                if *shutdown_rx.borrow_and_update() { break; }
                if shutdown_rx.changed().await.is_err() { break; }
            }
        } => {
            tracing::error!("self-shutdown triggered (all log dirs offline); stopping broker");
        }
    }
    // KIP-500 controlled shutdown: ask the controller to move leadership of
    // every partition this broker leads onto its other in-sync replicas
    // BEFORE we stop. This is the difference between a near-seamless failover
    // and stranding producers on a dead leader until their request timeout —
    // `kubectl delete pod` sends SIGTERM, and without this hand-off the
    // partition has no leader until the controller fences us (~tens of
    // seconds). Bounded well under the pod's terminationGracePeriod (30s); on
    // timeout `controlled_shutdown` falls back to a hard stop internally.
    match handle
        .controlled_shutdown(CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT)
        .await
    {
        Ok(()) => tracing::info!("controlled shutdown complete (leadership drained)"),
        Err(e) => tracing::warn!(error = %e, "controlled shutdown incomplete; hard-stopped"),
    }
    tracing::info!("crabka-broker stopped");
    telemetry.shutdown();
    Ok(())
}

/// How long controlled shutdown waits for the controller to drain this
/// broker's partition leaderships before falling back to a hard stop. Kept
/// well under the pod `terminationGracePeriodSeconds` (30s) so the hard-stop
/// fallback + process exit still complete before the Kubernetes SIGKILL.
const CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// Block until a process-termination signal arrives, returning its name for
/// logging. On Unix this is SIGINT (Ctrl-C) **or SIGTERM** — `kubectl delete
/// pod` (and the default container stop) sends SIGTERM, so catching only
/// SIGINT meant the broker was hard-killed by SIGKILL with no controlled
/// shutdown. On non-Unix targets, Ctrl-C only.
#[cfg(unix)]
async fn wait_for_termination_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut sigterm) => tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
        },
        Err(e) => {
            // Couldn't install the SIGTERM handler; fall back to SIGINT only
            // rather than refusing to start.
            tracing::warn!(error = %e, "failed to install SIGTERM handler; SIGINT only");
            let _ = tokio::signal::ctrl_c().await;
            "SIGINT"
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_termination_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "SIGINT"
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
    // Use the controller's own emptiness check (durable raft state =
    // `__cluster_metadata/quorum-state`, written only after the node has
    // participated in an election/commit) so this Bootstrap/Rejoin choice can
    // never disagree with `Controller::start_with_listener`'s mode validation.
    //
    // The bare `__cluster_metadata/@metadata-0` segment dir is created by
    // `KraftController::open` *before* the first commit. Keying Rejoin on its
    // existence (as we used to) bricked any node killed mid-election on a
    // multi-node cold start: the next boot saw the segment dir, picked Rejoin,
    // and died with "Rejoin requires non-empty raft log" — a crashloop. A node
    // with no persisted quorum-state now correctly re-Bootstraps.
    let metadata_dir = log_dir.join("__cluster_metadata");
    if crabka_raft::metadata_log_nonempty(&metadata_dir) {
        BootstrapMode::Rejoin
    } else {
        BootstrapMode::Bootstrap
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn detect_bootstrap_when_log_dir_is_empty() {
        let dir = tempdir().unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn parse_roles_arg_maps_strings() {
        assert!(
            parse_roles_arg(&["controller".to_string(), "broker".to_string()]).unwrap()
                == vec![
                    crabka_broker::config::NodeRole::Controller,
                    crabka_broker::config::NodeRole::Broker
                ]
        );
    }

    #[test]
    fn parse_roles_arg_rejects_unknown() {
        assert!(parse_roles_arg(&["nope".to_string()]).is_err());
    }

    #[test]
    fn detect_bootstrap_when_metadata_dir_missing() {
        let dir = tempdir().unwrap();
        // log_dir exists with unrelated content (bootstrap.json from
        // `crabka format`) but no __cluster_metadata/@metadata-0 subdir.
        std::fs::write(dir.path().join("bootstrap.json"), "{}").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_rejoin_when_quorum_state_persisted() {
        let dir = tempdir().unwrap();
        let meta = dir.path().join("__cluster_metadata");
        std::fs::create_dir_all(&meta).unwrap();
        // Durable raft state — `quorum-state` is written only after the node
        // has participated in an election/commit. This marks a true Rejoin.
        std::fs::write(meta.join("quorum-state"), b"{}").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Rejoin);
    }

    #[test]
    fn detect_bootstrap_when_segment_dir_but_no_quorum_state() {
        // Regression: a node killed mid-election on a multi-node cold start has
        // an `@metadata-0` segment dir (created by `KraftController::open`)
        // but no `quorum-state`. It must re-Bootstrap, not die in a Rejoin
        // crashloop. Previously this returned Rejoin and bricked the node.
        let dir = tempdir().unwrap();
        let meta = dir.path().join("__cluster_metadata").join("@metadata-0");
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(meta.join("00000000000000000000.log"), b"segment").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_metadata_dir_empty() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("__cluster_metadata").join("@metadata-0")).unwrap();
        // empty @metadata-0 dir is treated as no state (corner case:
        // crashed first start before any segment was written).
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_only_outer_cluster_metadata_dir_exists() {
        let dir = tempdir().unwrap();
        // The outer __cluster_metadata dir exists but the inner
        // @metadata-0 subdir doesn't — should still be Bootstrap.
        std::fs::create_dir_all(dir.path().join("__cluster_metadata")).unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn config_file_mutually_exclusive_with_listen_addr() {
        use clap::Parser;

        let res = Args::try_parse_from([
            "crabka-broker",
            "--config-file=/tmp/a.toml",
            "--listen-addr=127.0.0.1:9092",
        ]);
        let err = res.expect_err("expected mutual-exclusion error");
        let s = err.to_string();
        assert!(
            s.contains("config-file") && s.contains("listen-addr"),
            "expected clap conflict mentioning both flags, got: {s}"
        );
    }

    #[test]
    fn config_file_mutually_exclusive_with_advertised_listener() {
        use clap::Parser;

        let res = Args::try_parse_from([
            "crabka-broker",
            "--config-file=/tmp/a.toml",
            "--advertised-listener=h:9092",
        ]);
        let err = res.expect_err("expected mutual-exclusion error");
        let s = err.to_string();
        assert!(
            s.contains("config-file") && s.contains("advertised-listener"),
            "expected clap conflict, got: {s}"
        );
    }

    #[test]
    fn config_file_alone_parses() {
        use clap::Parser;

        let args = Args::try_parse_from(["crabka-broker", "--config-file=/tmp/a.toml"]).unwrap();
        assert!(args.config_file.as_deref() == Some(std::path::Path::new("/tmp/a.toml")));
        assert!(args.advertised_listener.is_none());
    }
}
