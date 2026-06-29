//! `crabka-rebalancer` — Cruise-Control-equivalent partition
//! rebalancer for Crabka clusters.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crabka_rebalancer::api::GoalRegistry;
use crabka_rebalancer::api::handlers::AppState;
use crabka_rebalancer::executor::client_impl::LiveClient;
use crabka_rebalancer::executor::{Execution, ExecutionHandle, ExecutorConfig, ExecutorState};
use crabka_rebalancer::goals::GoalContext;
use crabka_rebalancer::health::{HealthState, new_registry};
use crabka_rebalancer::ingest::{Ingester, new_shared_snapshot};
use crabka_rebalancer::metrics::RebalancerMetrics;
use crabka_rebalancer::model::proposal::ProposalStatus;
use crabka_rebalancer::model::store::ProposalStore;

fn should_warn_state_topic_load(is_loaded: bool) -> bool {
    !is_loaded
}

fn should_continue_recovery_load_wait(is_loaded: bool) -> bool {
    !is_loaded
}

fn recovery_load_timed_out(elapsed: Duration, timeout: Duration) -> bool {
    elapsed > timeout
}

fn detector_enabled(tick_interval_secs: u64) -> bool {
    tick_interval_secs > 0
}

#[derive(Debug, Parser)]
#[command(
    name = "crabka-rebalancer",
    version,
    about = "Cruise-Control-equivalent partition rebalancer"
)]
struct Args {
    /// `host:port,host:port,...` of brokers to use for bootstrap.
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,

    /// Bind address for the Connect-RPC + operational HTTP server.
    #[arg(
        long,
        env = "CRABKA_REBALANCER_LISTEN_ADDR",
        default_value = "0.0.0.0:9300"
    )]
    listen_addr: SocketAddr,

    /// Cluster-state snapshot cadence.
    #[arg(long, env = "CRABKA_SCRAPE_INTERVAL_SECS", default_value_t = 10)]
    scrape_interval_secs: u64,

    /// `(max - min) * 100 / total` must exceed this for soft goals to act.
    #[arg(long, env = "CRABKA_IMBALANCE_THRESHOLD_PCT", default_value_t = 10)]
    imbalance_threshold_pct: u32,

    /// Minimum leader count per (broker, topic) pair for the
    /// `MinTopicLeadersPerBroker` goal. `0` (default) disables it.
    #[arg(long, env = "CRABKA_MIN_TOPIC_LEADERS_PER_BROKER", default_value_t = 0)]
    min_topic_leaders_per_broker: u32,

    /// Safety cap on the total number of movements per proposal.
    #[arg(long, env = "CRABKA_MAX_MOVEMENTS_PER_PROPOSAL", default_value_t = 256)]
    max_movements_per_proposal: usize,

    /// In-memory ring buffer capacity for recent proposals.
    #[arg(long, env = "CRABKA_PROPOSAL_RING_BUFFER_SIZE", default_value_t = 20)]
    proposal_ring_buffer_size: usize,

    /// On-disk persistence directory. Created if missing.
    #[arg(
        long,
        env = "CRABKA_DATA_DIR",
        default_value = "/var/lib/crabka-rebalancer"
    )]
    data_dir: PathBuf,

    /// Optional path to a per-broker capacity YAML file. When unset,
    /// all five capacity goals are no-ops.
    #[arg(long, env = "CRABKA_BROKER_CAPACITY_FILE", default_value = "")]
    broker_capacity_file: String,

    /// Per-broker metric scrape targets. Format: "id:host:port,id:host:port,…".
    /// When set, overrides `--metrics-port` and uses these static targets
    /// instead of live discovery from the ingester's `Metadata` snapshot.
    /// Empty = fall back to discovered targets via `--metrics-port`.
    #[arg(long, env = "CRABKA_METRICS_SCRAPE_TARGETS", default_value = "")]
    metrics_scrape_targets: String,

    /// Broker metrics-endpoint port used by live scrape-target discovery.
    ///
    /// When `--metrics-scrape-targets` is unset, the scraper derives its
    /// target list from the ingester's `Metadata` snapshot, addressing
    /// each broker at `host:METRICS_PORT`. Ignored when
    /// `--metrics-scrape-targets` is set. Defaults to `crabka-broker`'s
    /// metrics port (`9404`).
    #[arg(long, env = "CRABKA_REBALANCER_METRICS_PORT", default_value_t = 9404)]
    metrics_port: u16,

    /// How often the scraper polls each target's /metrics endpoint.
    #[arg(
        long,
        env = "CRABKA_METRICS_SCRAPE_INTERVAL_SECS",
        default_value_t = 30
    )]
    metrics_scrape_interval_secs: u64,

    /// How long to retain scraped samples in the rolling window
    /// store. Default 12h matches the longest window (`TwelveHour`).
    #[arg(long, env = "CRABKA_METRICS_RETENTION_SECS", default_value_t = 43_200)]
    metrics_retention_secs: u64,

    /// How often the detector evaluates anomaly rules. `0` disables
    /// the detector entirely (no anomaly recording, no auto-trigger).
    #[arg(long, env = "CRABKA_DETECTOR_TICK_INTERVAL_SECS", default_value_t = 30)]
    detector_tick_interval_secs: u64,

    /// How long a broker must be absent from cluster snapshots before
    /// `BrokerDeath` fires.
    #[arg(
        long,
        env = "CRABKA_DETECTOR_BROKER_DEATH_THRESHOLD_SECS",
        default_value_t = 60
    )]
    detector_broker_death_threshold_secs: u64,

    /// How long ISR < replicas must persist before `UnderReplicatedPartitions` fires.
    #[arg(
        long,
        env = "CRABKA_DETECTOR_UNDER_REPLICATED_THRESHOLD_SECS",
        default_value_t = 120
    )]
    detector_under_replicated_threshold_secs: u64,

    /// Disk usage fraction (0.0..1.0) above which `DiskPressure` fires Warning.
    #[arg(
        long,
        env = "CRABKA_DETECTOR_DISK_PRESSURE_PCT",
        default_value_t = 0.85
    )]
    detector_disk_pressure_pct: f64,

    /// Disk usage fraction above which `DiskPressure` escalates to Critical.
    #[arg(
        long,
        env = "CRABKA_DETECTOR_DISK_CRITICAL_PCT",
        default_value_t = 0.95
    )]
    detector_disk_critical_pct: f64,

    /// `SlowBroker` multiplier (× cluster median CPU cores).
    #[arg(
        long,
        env = "CRABKA_DETECTOR_SLOW_BROKER_MULTIPLIER",
        default_value_t = 2.0
    )]
    detector_slow_broker_multiplier: f64,

    /// `SlowBroker` absolute minimum cores floor. Prevents false-positives
    /// on idle clusters where the multiplier threshold is near zero.
    #[arg(
        long,
        env = "CRABKA_DETECTOR_SLOW_BROKER_MIN_CORES",
        default_value_t = 0.5
    )]
    detector_slow_broker_min_cores: f64,

    /// Default mute window applied after an anomaly auto-triggers a proposal.
    #[arg(long, env = "CRABKA_DETECTOR_MUTE_WINDOW_SECS", default_value_t = 900)]
    detector_mute_window_secs: u64,

    /// Master switch on auto-trigger. When false, the detector still
    /// records + surfaces anomalies but never creates a proposal.
    /// Default false — operators must opt in.
    #[arg(
        long,
        env = "CRABKA_DETECTOR_AUTO_TRIGGER_ENABLED",
        default_value_t = false
    )]
    detector_auto_trigger_enabled: bool,

    /// In-memory + on-disk ring buffer size for anomaly history at
    /// `{data_dir}/anomalies.json`.
    #[arg(long, env = "CRABKA_ANOMALY_RING_BUFFER_SIZE", default_value_t = 200)]
    anomaly_ring_buffer_size: usize,

    /// Name of the internal compacted topic the rebalancer uses to
    /// persist executor state. Survives pod restart. Created on first
    /// startup with `cleanup.policy=compact`, single partition.
    #[arg(
        long,
        env = "CRABKA_REBALANCER_STATE_TOPIC",
        default_value = "__crabka_rebalancer_state"
    )]
    state_topic_name: String,

    /// Replication factor for the state topic at create time. On
    /// `INVALID_REPLICATION_FACTOR` the binary retries topic creation
    /// with RF=1 to support single-broker dev clusters.
    #[arg(
        long,
        env = "CRABKA_REBALANCER_STATE_TOPIC_REPLICATION",
        default_value_t = 3
    )]
    state_topic_replication: i16,

    /// Soft deadline for state-topic load at startup; the loader emits
    /// a WARN and keeps retrying past this. `/readyz` stays 503 until
    /// the load completes successfully.
    #[arg(
        long,
        env = "CRABKA_REBALANCER_STATE_LOAD_TIMEOUT_SECS",
        default_value_t = 60
    )]
    state_load_timeout_secs: u64,

    /// Default KIP-73 throttle (bytes/sec, per broker direction) when
    /// `ExecuteProposalRequest.throttle_bytes_per_sec` is unset.
    #[arg(
        long,
        env = "CRABKA_DEFAULT_THROTTLE_BYTES_PER_SEC",
        default_value_t = 50_000_000
    )]
    default_throttle_bytes_per_sec: i64,

    /// Per-execution deadline before the executor cancels in-flight
    /// reassignments and fails the proposal.
    #[arg(long, env = "CRABKA_EXECUTE_DEADLINE_SECS", default_value_t = 1800)]
    execute_deadline_secs: u64,

    /// How often the executor polls `ListPartitionReassignments` during
    /// the Wait phase.
    #[arg(
        long,
        env = "CRABKA_REASSIGNMENT_POLL_INTERVAL_SECS",
        default_value_t = 5
    )]
    reassignment_poll_interval_secs: u64,

    /// Maximum movements per `AlterPartitionReassignments` request.
    #[arg(long, env = "CRABKA_REASSIGNMENT_BATCH_SIZE", default_value_t = 200)]
    reassignment_batch_size: usize,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_rebalancer=info,info".into()),
        )
        .init();

    let args = Args::parse();
    info!(
        listen = %args.listen_addr,
        bootstrap = %args.bootstrap_servers,
        data_dir = ?args.data_dir,
        "crabka-rebalancer starting"
    );

    std::fs::create_dir_all(&args.data_dir)?;

    let client = crabka_client_core::Client::builder()
        .bootstrap(args.bootstrap_servers.clone())
        .client_id("crabka-rebalancer")
        .build()
        .await?;

    let snapshot = new_shared_snapshot();
    let shutdown = CancellationToken::new();

    let mut registry = new_registry();
    let metrics = RebalancerMetrics::register(&mut registry);
    let detector_metrics = crabka_rebalancer::detector::DetectorMetrics::register(&mut registry);
    let registry = Arc::new(Mutex::new(registry));

    let store = Arc::new(ProposalStore::open(
        &args.data_dir,
        args.proposal_ring_buffer_size,
    )?);

    let ingester = Ingester::new(
        client.clone(),
        Duration::from_secs(args.scrape_interval_secs),
        snapshot.clone(),
        shutdown.clone(),
        metrics.clone(),
    );
    let ingester_handle = tokio::spawn(ingester.run());

    let executor_config = ExecutorConfig {
        data_dir: args.data_dir.clone(),
        default_throttle_bytes_per_sec: args.default_throttle_bytes_per_sec,
        poll_interval: Duration::from_secs(args.reassignment_poll_interval_secs),
        execute_deadline: Duration::from_secs(args.execute_deadline_secs),
        batch_size: args.reassignment_batch_size,
    };

    let in_flight_slot: Arc<Mutex<Option<ExecutionHandle>>> = Arc::new(Mutex::new(None));

    // Ensure the state topic exists; spawn the background loader.
    // `topic_admin::ensure_topic` takes `&mut crabka_client_admin::AdminClient`.
    // We connect a short-lived admin client just for topic creation; the
    // `StateTopic` and `StateTopicLoader` then run on the main `Client`.
    {
        let addrs: Vec<String> = args
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        let mut admin = crabka_client_admin::AdminClient::connect(&addrs)
            .await
            .map_err(|e| anyhow::anyhow!("admin client connect: {e}"))?;
        crabka_rebalancer::state_topic::topic_admin::ensure_topic(
            &mut admin,
            &args.state_topic_name,
            args.state_topic_replication,
        )
        .await
        .map_err(|e| anyhow::anyhow!("ensure state topic: {e}"))?;
    }

    let arc_client = Arc::new(client.clone());
    let loaded_state = crabka_rebalancer::state_topic::LoadedState::new();
    let state_topic: Arc<dyn crabka_rebalancer::state_topic::StateBackend> =
        Arc::new(crabka_rebalancer::state_topic::StateTopic::new(
            arc_client.clone(),
            args.state_topic_name.clone(),
            loaded_state.clone(),
        ));

    let loader = crabka_rebalancer::state_topic::StateTopicLoader {
        client: arc_client.clone(),
        topic: args.state_topic_name.clone(),
        state: loaded_state.clone(),
        shutdown: shutdown.clone(),
    };
    tokio::spawn(loader.run());

    // Soft deadline: warn if the loader hasn't converged within the configured
    // timeout. The loader keeps retrying; /readyz stays 503 until it finishes.
    {
        let warn_state = loaded_state.clone();
        let timeout_secs = args.state_load_timeout_secs;
        let topic_for_warn = args.state_topic_name.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(timeout_secs)).await;
            if should_warn_state_topic_load(warn_state.is_loaded()) {
                warn!(
                    topic = %topic_for_warn,
                    timeout_secs,
                    "state topic has not loaded within the soft deadline; /readyz will remain 503"
                );
            }
        });
    }

    info!(
        topic = %args.state_topic_name,
        "state topic ready; loader spawned"
    );

    let executor_state = ExecutorState {
        store: store.clone(),
        config: executor_config,
        metrics: metrics.clone(),
        in_flight: in_flight_slot.clone(),
        state_topic: state_topic.clone(),
    };

    let live_client: Arc<dyn crabka_rebalancer::executor::phases::ClientFacade> =
        Arc::new(LiveClient::new(client.clone()));

    // Recovery on startup: resume an in-flight execution from the state topic.
    // We spawn a background task that polls until the loader has finished its
    // initial replay (is_loaded() == true), then checks for a stored record.
    // This preserves fast boot-to-/healthz latency — recovery happens
    // out-of-band of the synchronous startup path.
    tokio::spawn({
        let state_topic = state_topic.clone();
        let store = store.clone();
        let in_flight_slot = in_flight_slot.clone();
        let exec_state = executor_state.clone();
        let exec_client = live_client.clone();
        let shutdown = shutdown.clone();
        let load_timeout = Duration::from_secs(args.state_load_timeout_secs);
        async move {
            let start = std::time::Instant::now();
            while should_continue_recovery_load_wait(state_topic.is_loaded()) {
                if recovery_load_timed_out(start.elapsed(), load_timeout) {
                    warn!(
                        timeout_secs = load_timeout.as_secs(),
                        "state-topic load did not converge within timeout; \
                         skipping in-flight recovery"
                    );
                    return;
                }
                if shutdown.is_cancelled() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if let Some(in_flight) = state_topic.loaded() {
                info!(
                    proposal_id = %in_flight.proposal_id,
                    phase = ?in_flight.phase,
                    "resuming in-flight executor state from state topic"
                );
                if let Some(proposal) = store.get(&in_flight.proposal_id) {
                    let prop_for_resume = store
                        .mutate(&in_flight.proposal_id, |p| {
                            p.status = ProposalStatus::Executing;
                        })
                        .unwrap_or(proposal);
                    let cancel = CancellationToken::new();
                    let handle_cancel = cancel.clone();
                    let in_flight_for_resume = in_flight.clone();
                    let task = tokio::spawn(async move {
                        Execution::resume(
                            exec_client,
                            exec_state,
                            prop_for_resume,
                            &in_flight_for_resume,
                            cancel,
                        )
                        .run()
                        .await;
                    });
                    *in_flight_slot.lock().await = Some(ExecutionHandle {
                        proposal_id: in_flight.proposal_id.clone(),
                        task,
                        cancel: handle_cancel,
                        started_at: std::time::Instant::now(),
                    });
                } else {
                    warn!(
                        proposal_id = %in_flight.proposal_id,
                        "state topic references unknown proposal; clearing"
                    );
                    let _ = state_topic.delete().await;
                }
            }
        }
    });

    // Load broker capacity config (optional).
    let broker_capacities = if args.broker_capacity_file.is_empty() {
        std::sync::Arc::new(crabka_rebalancer::capacity::BrokerCapacities::default())
    } else {
        match crabka_rebalancer::capacity::load::load_from_path(std::path::Path::new(
            &args.broker_capacity_file,
        )) {
            Ok(c) => {
                let mut broker_ids: Vec<i32> = c.by_broker.keys().copied().collect();
                broker_ids.sort_unstable();
                info!(
                    path = %args.broker_capacity_file,
                    broker_count = c.by_broker.len(),
                    broker_ids = ?broker_ids,
                    "loaded broker capacity config"
                );
                std::sync::Arc::new(c)
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "failed to load broker capacity file `{}`: {e}",
                    args.broker_capacity_file
                ));
            }
        }
    };

    let usage_store = std::sync::Arc::new(crabka_rebalancer::scraper::UsageStore::new(
        crabka_rebalancer::scraper::WindowConfig {
            scrape_interval: std::time::Duration::from_secs(args.metrics_scrape_interval_secs),
            retention: std::time::Duration::from_secs(args.metrics_retention_secs),
        },
    ));

    let source: crabka_rebalancer::scraper::TargetSource =
        if args.metrics_scrape_targets.trim().is_empty() {
            info!(
                metrics_port = args.metrics_port,
                scrape_interval_secs = args.metrics_scrape_interval_secs,
                retention_secs = args.metrics_retention_secs,
                "starting metrics scraper (discovered targets via Metadata)"
            );
            crabka_rebalancer::scraper::TargetSource::Discovered {
                snapshot: snapshot.clone(),
                metrics_port: args.metrics_port,
            }
        } else {
            let targets = crabka_rebalancer::scraper::parse_targets(&args.metrics_scrape_targets)
                .map_err(|e| {
                anyhow::anyhow!(
                    "failed to parse --metrics-scrape-targets `{}`: {e}",
                    args.metrics_scrape_targets
                )
            })?;
            info!(
                target_count = targets.len(),
                scrape_interval_secs = args.metrics_scrape_interval_secs,
                retention_secs = args.metrics_retention_secs,
                "starting metrics scraper (static targets)"
            );
            crabka_rebalancer::scraper::TargetSource::Static(targets)
        };

    let scraper = crabka_rebalancer::scraper::Scraper::new(
        source,
        std::time::Duration::from_secs(args.metrics_scrape_interval_secs),
        usage_store.clone(),
        shutdown.clone(),
    );
    tokio::spawn(scraper.run());

    let goal_registry = Arc::new(GoalRegistry::default_registry());

    let anomaly_store = Arc::new(crabka_rebalancer::detector::AnomalyStore::open(
        &args.data_dir,
        args.anomaly_ring_buffer_size,
    )?);

    let goal_ctx = GoalContext {
        imbalance_threshold_pct: args.imbalance_threshold_pct,
        max_movements_per_proposal: args.max_movements_per_proposal,
        min_topic_leaders_per_broker: args.min_topic_leaders_per_broker,
        broker_capacities: broker_capacities.clone(),
        broker_usages: usage_store.clone(),
    };

    if detector_enabled(args.detector_tick_interval_secs) {
        let detector_cfg = crabka_rebalancer::detector::DetectorConfig {
            tick_interval: Duration::from_secs(args.detector_tick_interval_secs),
            broker_death_threshold: Duration::from_secs(args.detector_broker_death_threshold_secs),
            under_replicated_threshold: Duration::from_secs(
                args.detector_under_replicated_threshold_secs,
            ),
            disk_pressure_pct: args.detector_disk_pressure_pct,
            disk_critical_pct: args.detector_disk_critical_pct,
            slow_broker_multiplier: args.detector_slow_broker_multiplier,
            slow_broker_min_cores: args.detector_slow_broker_min_cores,
            default_mute_window: Duration::from_secs(args.detector_mute_window_secs),
            auto_trigger_enabled: args.detector_auto_trigger_enabled,
            history_capacity: 10,
        };
        info!(
            tick_secs = args.detector_tick_interval_secs,
            auto_trigger = args.detector_auto_trigger_enabled,
            broker_death_threshold_secs = args.detector_broker_death_threshold_secs,
            "starting detector"
        );
        let detector = crabka_rebalancer::detector::Detector::new(
            detector_cfg,
            snapshot.clone(),
            usage_store.clone(),
            broker_capacities.clone(),
            anomaly_store.clone(),
            store.clone(),
            executor_state.clone(),
            goal_registry.clone(),
            goal_ctx.clone(),
            detector_metrics,
            shutdown.clone(),
        );
        tokio::spawn(detector.run());
    }

    let app_state = Arc::new(AppState {
        snapshot: snapshot.clone(),
        store,
        goal_registry: goal_registry.clone(),
        goal_ctx,
        metrics: metrics.clone(),
        executor: executor_state,
        client_facade: live_client,
        anomaly_store: anomaly_store.clone(),
        state_topic: state_topic.clone(),
    });

    let connect_router = crabka_rebalancer::api::router(app_state);
    let health_router = crabka_rebalancer::health::router(HealthState {
        snapshot: snapshot.clone(),
        registry,
        state_topic,
    });
    let app = connect_router.merge(health_router);

    let listener = tokio::net::TcpListener::bind(args.listen_addr).await?;
    info!(addr = %listener.local_addr()?, "listening");
    let shutdown_for_axum = shutdown.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown_for_axum.cancel();
        })
        .await?;

    // Drain the in-flight executor task, if any. The cancel + clear path
    // runs in the executor's run() loop and reaches ClearThrottle for
    // every terminal — bounded by execute_deadline + clear_throttle RPC
    // time, so a 10s timeout is generous.
    if let Some(handle) = in_flight_slot.lock().await.take() {
        info!(proposal_id = %handle.proposal_id, "draining in-flight executor on shutdown");
        handle.cancel.cancel();
        match tokio::time::timeout(Duration::from_secs(10), handle.task).await {
            Ok(Ok(())) => info!(proposal_id = %handle.proposal_id, "executor drained cleanly"),
            Ok(Err(e)) => warn!(error = %e, "executor task join error"),
            Err(_) => {
                warn!(proposal_id = %handle.proposal_id, "executor drain timed out after 10s; aborting");
            }
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(5), ingester_handle).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_topic_load_warning_only_before_loaded() {
        assert!(should_warn_state_topic_load(false));
        assert!(!should_warn_state_topic_load(true));
    }

    #[test]
    fn recovery_load_wait_continues_only_before_loaded() {
        assert!(should_continue_recovery_load_wait(false));
        assert!(!should_continue_recovery_load_wait(true));
    }

    #[test]
    fn recovery_load_timeout_is_strictly_after_deadline() {
        let timeout = Duration::from_secs(5);
        assert!(!recovery_load_timed_out(Duration::from_secs(5), timeout));
        assert!(recovery_load_timed_out(
            Duration::from_secs(5) + Duration::from_millis(1),
            timeout
        ));
    }

    #[test]
    fn detector_is_disabled_only_at_zero_interval() {
        assert!(!detector_enabled(0));
        assert!(detector_enabled(1));
    }
}
