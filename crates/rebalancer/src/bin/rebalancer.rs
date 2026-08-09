//! `crabka-rebalancer`: a Cruise-Control-equivalent partition
//! rebalancer for Crabka clusters.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, ConnectionOptions,
    DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use crabka_rebalancer::{
    api::{GoalRegistry, handlers::AppState},
    config::{PositiveUsize, RebalancerRuntimePolicy},
    executor::{
        Execution, ExecutionHandle, ExecutorConfig, ExecutorState,
        client_impl::{LiveClient, ReassignmentRequestTimeout},
    },
    goals::GoalContext,
    health::{HealthState, new_registry},
    ingest::{Ingester, new_shared_snapshot},
    metrics::RebalancerMetrics,
    model::{proposal::ProposalStatus, store::ProposalStore},
};
use crabka_units::{
    ByteRate, ByteSize, Ratio, Time,
    convert::{ByteRateExt as _, StdDurationExt as _, TimeExt as _},
    fraction, parse, percent,
};
#[cfg(test)]
use crabka_units::{millis, secs};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

fn should_warn_state_topic_load(is_loaded: bool) -> bool {
    !is_loaded
}

fn should_continue_recovery_load_wait(is_loaded: bool) -> bool {
    !is_loaded
}

fn recovery_load_timed_out(elapsed: Time, timeout: Time) -> bool {
    elapsed > timeout
}

fn detector_enabled(tick_interval: Time) -> bool {
    tick_interval > Time::ZERO
}

/// A `--…-secs` CLI argument as a [`Time`] extent.
///
/// The flag names carry the unit because they are the operator-facing
/// contract. The quantity carries the unit from here on. A value too large
/// for `i64` seconds saturates; it does not wrap.
fn arg_secs(value: u64) -> Time {
    Time::from_secs(i64::try_from(value).unwrap_or(i64::MAX))
}

/// A `--…-pct` CLI argument given as a whole percentage.
fn arg_percent(value: u32) -> Ratio {
    percent(value)
}

/// A `--…-pct` CLI argument given as a `0.0..=1.0` fraction.
fn arg_fraction(value: f64) -> Ratio {
    fraction(value)
}

/// A `--…-bytes-per-sec` CLI argument as a [`ByteRate`].
fn arg_bytes_per_sec(value: i64) -> ByteRate {
    ByteRate::from_bytes_per_sec(value)
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_rebalancer=info,info".into()),
        )
        .init();
}

fn prepare_data_dir(args: &Args) -> anyhow::Result<()> {
    info!(
        listen = %args.listen_addr,
        bootstrap = %args.bootstrap_servers,
        data_dir = ?args.data_dir,
        "crabka-rebalancer starting"
    );
    std::fs::create_dir_all(&args.data_dir)?;
    Ok(())
}

async fn connect_client(args: &Args) -> anyhow::Result<crabka_client_core::Client> {
    Ok(crabka_client_core::Client::builder()
        .bootstrap(args.bootstrap_servers.clone())
        .client_id("crabka-rebalancer")
        .dispatch_queue_capacity(args.client_dispatch_queue_capacity)
        .frame_max(args.client_frame_max)
        .build()
        .await?)
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
    #[arg(
        long,
        env = "CRABKA_REBALANCER_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_REBALANCER_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,

    #[command(flatten)]
    runtime: RebalancerRuntimeOptions,

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

    /// Optional path to a per-broker capacity YAML file. When unset, all five
    /// capacity goals are no-ops.
    #[arg(long, env = "CRABKA_BROKER_CAPACITY_FILE", default_value = "")]
    broker_capacity_file: String,

    /// Per-broker metric scrape targets. Format: "id:host:port,id:host:port,…".
    /// When set, overrides `--metrics-port` and uses these static targets
    /// instead of live discovery from the ingester's `Metadata` snapshot.
    /// An empty value falls back to targets discovered with `--metrics-port`.
    #[arg(long, env = "CRABKA_METRICS_SCRAPE_TARGETS", default_value = "")]
    metrics_scrape_targets: String,

    /// Broker metrics-endpoint port used by live scrape-target discovery.
    ///
    /// When `--metrics-scrape-targets` is unset, the scraper derives its
    /// target list from the ingester's `Metadata` snapshot and addresses
    /// each broker at `host:METRICS_PORT`. The scraper ignores this port
    /// when `--metrics-scrape-targets` is set. Default: `crabka-broker`'s
    /// metrics port `9404`.
    #[arg(long, env = "CRABKA_REBALANCER_METRICS_PORT", default_value_t = 9404)]
    metrics_port: u16,

    /// How often the scraper polls each target's /metrics endpoint.
    #[arg(
        long,
        env = "CRABKA_METRICS_SCRAPE_INTERVAL_SECS",
        default_value_t = 30
    )]
    metrics_scrape_interval_secs: u64,

    /// How long to retain scraped samples in the rolling window store. The
    /// default of 12h matches the longest window, `TwelveHour`.
    #[arg(long, env = "CRABKA_METRICS_RETENTION_SECS", default_value_t = 43_200)]
    metrics_retention_secs: u64,

    /// How often the detector evaluates anomaly rules. `0` disables the
    /// detector entirely: it records no anomaly and runs no auto-trigger.
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

    /// Disk usage fraction in the range 0.0..1.0 above which `DiskPressure`
    /// fires Warning.
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

    /// `SlowBroker` absolute minimum cores floor. It prevents false positives
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
    /// records and surfaces anomalies, but it never creates a proposal.
    /// Default: false. Operators must opt in.
    #[arg(
        long,
        env = "CRABKA_DETECTOR_AUTO_TRIGGER_ENABLED",
        default_value_t = false
    )]
    detector_auto_trigger_enabled: bool,

    /// In-memory and on-disk ring buffer size for anomaly history at
    /// `{data_dir}/anomalies.json`.
    #[arg(long, env = "CRABKA_ANOMALY_RING_BUFFER_SIZE", default_value_t = 200)]
    anomaly_ring_buffer_size: usize,

    /// Name of the internal compacted topic the rebalancer uses to persist
    /// executor state. The topic survives a pod restart. The binary creates it
    /// on first startup with `cleanup.policy=compact` and a single partition.
    #[arg(
        long,
        env = "CRABKA_REBALANCER_STATE_TOPIC",
        default_value = "__crabka_rebalancer_state"
    )]
    state_topic_name: String,

    /// Replication factor for the state topic at create time. On
    /// `INVALID_REPLICATION_FACTOR` the binary retries topic creation
    /// with RF=1, to support single-broker dev clusters.
    #[arg(
        long,
        env = "CRABKA_REBALANCER_STATE_TOPIC_REPLICATION",
        default_value_t = 3
    )]
    state_topic_replication: i16,

    /// Soft deadline for state-topic load at startup. The loader emits a WARN
    /// and keeps retrying past this deadline. `/readyz` stays 503 until the
    /// load completes successfully.
    #[arg(
        long,
        env = "CRABKA_REBALANCER_STATE_LOAD_TIMEOUT_SECS",
        default_value_t = 60
    )]
    state_load_timeout_secs: u64,

    /// Default KIP-73 throttle in bytes/sec, per broker direction, used when
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

    /// Kafka broker-side timeout for submitting or cancelling partition
    /// reassignments.
    #[arg(
        long,
        env = "CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT",
        default_value = "60s",
        value_parser = crabka_units::parse::positive_time
    )]
    reassignment_request_timeout: Time,
}

#[derive(Debug, clap::Args, Default)]
struct RebalancerRuntimeOptions {
    #[arg(long, env = "CRABKA_REBALANCER_RECOVERY_LOAD_POLL_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    recovery_load_poll_interval: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_EXECUTOR_DRAIN_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    executor_drain_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_INGESTER_JOIN_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    ingester_join_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_SCRAPER_HTTP_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    scraper_http_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_CANCEL_DRAIN_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    cancel_drain_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_CANCEL_DRAIN_POLL_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    cancel_drain_poll_interval: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_DETECTOR_HISTORY_CAPACITY")]
    detector_history_capacity: Option<PositiveUsize>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_TOPIC_CREATE_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    state_topic_create_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_LOADER_POLL_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    state_loader_poll_interval: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_LOADER_QUIET_POLLS")]
    state_loader_quiet_polls: Option<PositiveUsize>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_FETCH_MAX", value_parser = crabka_units::parse::positive_byte_size)]
    state_fetch_max: Option<ByteSize>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_PRODUCE_RETRY_ATTEMPTS")]
    state_produce_retry_attempts: Option<PositiveUsize>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_PRODUCE_RETRY_BACKOFF", value_parser = crabka_units::parse::positive_time)]
    state_produce_retry_backoff: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_PRODUCE_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    state_produce_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_TOPIC_MIN_CLEANABLE_DIRTY_RATIO", value_parser = crabka_units::parse::positive_ratio)]
    state_topic_min_cleanable_dirty_ratio: Option<Ratio>,
    #[arg(long, env = "CRABKA_REBALANCER_STATE_TOPIC_SEGMENT_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    state_topic_segment_interval: Option<Time>,
}

impl RebalancerRuntimeOptions {
    fn effective_policy(&self) -> anyhow::Result<RebalancerRuntimePolicy> {
        let defaults = RebalancerRuntimePolicy::default();
        let policy = RebalancerRuntimePolicy {
            recovery_load_poll_interval: self
                .recovery_load_poll_interval
                .unwrap_or(defaults.recovery_load_poll_interval),
            executor_drain_timeout: self
                .executor_drain_timeout
                .unwrap_or(defaults.executor_drain_timeout),
            ingester_join_timeout: self
                .ingester_join_timeout
                .unwrap_or(defaults.ingester_join_timeout),
            scraper_http_timeout: self
                .scraper_http_timeout
                .unwrap_or(defaults.scraper_http_timeout),
            cancel_drain_timeout: self
                .cancel_drain_timeout
                .unwrap_or(defaults.cancel_drain_timeout),
            cancel_drain_poll_interval: self
                .cancel_drain_poll_interval
                .unwrap_or(defaults.cancel_drain_poll_interval),
            detector_history_capacity: self
                .detector_history_capacity
                .unwrap_or(defaults.detector_history_capacity),
            state_topic_create_timeout: self
                .state_topic_create_timeout
                .unwrap_or(defaults.state_topic_create_timeout),
            state_loader_poll_interval: self
                .state_loader_poll_interval
                .unwrap_or(defaults.state_loader_poll_interval),
            state_loader_quiet_polls: self
                .state_loader_quiet_polls
                .unwrap_or(defaults.state_loader_quiet_polls),
            state_fetch_max: self.state_fetch_max.unwrap_or(defaults.state_fetch_max),
            state_produce_retry_attempts: self
                .state_produce_retry_attempts
                .unwrap_or(defaults.state_produce_retry_attempts),
            state_produce_retry_backoff: self
                .state_produce_retry_backoff
                .unwrap_or(defaults.state_produce_retry_backoff),
            state_produce_timeout: self
                .state_produce_timeout
                .unwrap_or(defaults.state_produce_timeout),
            state_topic_min_cleanable_dirty_ratio: self
                .state_topic_min_cleanable_dirty_ratio
                .unwrap_or(defaults.state_topic_min_cleanable_dirty_ratio),
            state_topic_segment_interval: self
                .state_topic_segment_interval
                .unwrap_or(defaults.state_topic_segment_interval),
        };
        policy.validate().map_err(anyhow::Error::msg)?;
        Ok(policy)
    }
}

struct StateTopicSetup {
    backend: Arc<dyn crabka_rebalancer::state_topic::StateBackend>,
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

async fn start_state_topic(
    args: &Args,
    client: &crabka_client_core::Client,
    shutdown: &CancellationToken,
    runtime_policy: RebalancerRuntimePolicy,
) -> anyhow::Result<StateTopicSetup> {
    let addrs: Vec<String> = args
        .bootstrap_servers
        .split(',')
        .map(|address| address.trim().to_string())
        .collect();
    let mut admin = crabka_client_admin::AdminClient::connect_with_options(
        &addrs,
        ConnectionOptions {
            client_id: "crabka-rebalancer".to_owned(),
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::new(
                args.client_dispatch_queue_capacity,
            )
            .expect("validated by clap"),
            frame_max: ClientFrameMax::try_from(args.client_frame_max).expect("validated by clap"),
            ..ConnectionOptions::default()
        },
    )
    .await
    .map_err(|error| anyhow::anyhow!("admin client connect: {error}"))?;
    crabka_rebalancer::state_topic::topic_admin::ensure_topic_with_policy(
        &mut admin,
        &args.state_topic_name,
        args.state_topic_replication,
        &runtime_policy,
    )
    .await
    .map_err(|error| anyhow::anyhow!("ensure state topic: {error}"))?;

    let client = Arc::new(client.clone());
    let loaded = crabka_rebalancer::state_topic::LoadedState::new();
    let backend: Arc<dyn crabka_rebalancer::state_topic::StateBackend> =
        Arc::new(crabka_rebalancer::state_topic::StateTopic::new_with_policy(
            Arc::clone(&client),
            args.state_topic_name.clone(),
            loaded.clone(),
            runtime_policy,
        ));
    let state_loader = crabka_rebalancer::state_topic::StateTopicLoader {
        client,
        topic: args.state_topic_name.clone(),
        state: loaded.clone(),
        shutdown: shutdown.clone(),
        runtime_policy,
    };
    tokio::spawn(state_loader.run());

    let warn_state = loaded.clone();
    let timeout_secs = args.state_load_timeout_secs;
    let load_timeout = arg_secs(timeout_secs);
    let topic = args.state_topic_name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(load_timeout.to_std()).await;
        if should_warn_state_topic_load(warn_state.is_loaded()) {
            warn!(
                %topic,
                timeout_secs,
                "state topic has not loaded within the soft deadline; /readyz will remain 503"
            );
        }
    });

    info!(topic = %args.state_topic_name, "state topic ready; loader spawned");
    Ok(StateTopicSetup { backend })
}

fn spawn_recovery(
    state_topic: Arc<dyn crabka_rebalancer::state_topic::StateBackend>,
    store: Arc<ProposalStore>,
    in_flight_slot: Arc<Mutex<Option<ExecutionHandle>>>,
    executor_state: ExecutorState,
    client: Arc<dyn crabka_rebalancer::executor::phases::ClientFacade>,
    shutdown: CancellationToken,
    load_policy: (Time, Time),
) {
    tokio::spawn(async move {
        let (load_timeout, load_poll_interval) = load_policy;
        let start = std::time::Instant::now();
        while should_continue_recovery_load_wait(state_topic.is_loaded()) {
            if recovery_load_timed_out(start.elapsed().as_time(), load_timeout) {
                warn!(
                    timeout_secs = load_timeout.secs_f64(),
                    "state-topic load did not converge within timeout; skipping in-flight recovery"
                );
                return;
            }
            if shutdown.is_cancelled() {
                return;
            }
            tokio::time::sleep(load_poll_interval.to_std()).await;
        }
        let Some(in_flight) = state_topic.loaded() else {
            return;
        };
        info!(
            proposal_id = %in_flight.proposal_id,
            phase = ?in_flight.phase,
            "resuming in-flight executor state from state topic"
        );
        let Some(proposal) = store.get(&in_flight.proposal_id) else {
            warn!(
                proposal_id = %in_flight.proposal_id,
                "state topic references unknown proposal; clearing"
            );
            let _ = state_topic.delete().await;
            return;
        };
        let proposal = store
            .mutate(&in_flight.proposal_id, |proposal| {
                proposal.status = ProposalStatus::Executing;
            })
            .unwrap_or(proposal);
        let cancel = CancellationToken::new();
        let handle_cancel = cancel.clone();
        let resumed = in_flight.clone();
        let task = tokio::spawn(async move {
            Execution::resume(client, executor_state, proposal, &resumed, cancel)
                .run()
                .await;
        });
        *in_flight_slot.lock().await = Some(ExecutionHandle {
            proposal_id: in_flight.proposal_id.clone(),
            task,
            cancel: handle_cancel,
            started_at: std::time::Instant::now(),
        });
    });
}

async fn drain_execution(in_flight_slot: &Mutex<Option<ExecutionHandle>>, drain_timeout: Time) {
    let Some(handle) = in_flight_slot.lock().await.take() else {
        return;
    };
    info!(proposal_id = %handle.proposal_id, "draining in-flight executor on shutdown");
    handle.cancel.cancel();
    match tokio::time::timeout(drain_timeout.to_std(), handle.task).await {
        Ok(Ok(())) => info!(proposal_id = %handle.proposal_id, "executor drained cleanly"),
        Ok(Err(error)) => warn!(%error, "executor task join error"),
        Err(_) => {
            warn!(proposal_id = %handle.proposal_id, timeout_secs = drain_timeout.secs_f64(), "executor drain timed out; aborting");
        }
    }
}

async fn finish_shutdown(
    in_flight_slot: &Mutex<Option<ExecutionHandle>>,
    ingester_handle: tokio::task::JoinHandle<()>,
    policy: RebalancerRuntimePolicy,
) {
    drain_execution(in_flight_slot, policy.executor_drain_timeout).await;
    let _ = tokio::time::timeout(policy.ingester_join_timeout.to_std(), ingester_handle).await;
}

fn scraper_target_source(
    args: &Args,
    snapshot: crabka_rebalancer::ingest::SharedSnapshot,
) -> anyhow::Result<crabka_rebalancer::scraper::TargetSource> {
    if args.metrics_scrape_targets.trim().is_empty() {
        info!(
            metrics_port = args.metrics_port,
            scrape_interval_secs = args.metrics_scrape_interval_secs,
            retention_secs = args.metrics_retention_secs,
            "starting metrics scraper (discovered targets via Metadata)"
        );
        Ok(crabka_rebalancer::scraper::TargetSource::Discovered {
            snapshot,
            metrics_port: args.metrics_port,
        })
    } else {
        let targets = crabka_rebalancer::scraper::parse_targets(&args.metrics_scrape_targets)
            .map_err(|error| {
                anyhow::anyhow!(
                    "failed to parse --metrics-scrape-targets `{}`: {error}",
                    args.metrics_scrape_targets
                )
            })?;
        info!(
            target_count = targets.len(),
            scrape_interval_secs = args.metrics_scrape_interval_secs,
            retention_secs = args.metrics_retention_secs,
            "starting metrics scraper (static targets)"
        );
        Ok(crabka_rebalancer::scraper::TargetSource::Static(targets))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let runtime_policy = args.runtime.effective_policy()?;
    let reassignment_request_timeout =
        ReassignmentRequestTimeout::new(args.reassignment_request_timeout)
            .map_err(anyhow::Error::msg)?;
    prepare_data_dir(&args)?;
    let client = connect_client(&args).await?;
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
        arg_secs(args.scrape_interval_secs),
        snapshot.clone(),
        shutdown.clone(),
        metrics.clone(),
    );
    let ingester_handle = tokio::spawn(ingester.run());

    let executor_config = ExecutorConfig {
        data_dir: args.data_dir.clone(),
        default_throttle: arg_bytes_per_sec(args.default_throttle_bytes_per_sec),
        poll_interval: arg_secs(args.reassignment_poll_interval_secs),
        execute_deadline: arg_secs(args.execute_deadline_secs),
        batch_size: args.reassignment_batch_size,
    };

    let in_flight_slot: Arc<Mutex<Option<ExecutionHandle>>> = Arc::new(Mutex::new(None));

    let StateTopicSetup {
        backend: state_topic,
    } = start_state_topic(&args, &client, &shutdown, runtime_policy).await?;

    let executor_state = ExecutorState {
        store: store.clone(),
        config: executor_config,
        metrics: metrics.clone(),
        in_flight: in_flight_slot.clone(),
        state_topic: state_topic.clone(),
    };

    let live_client: Arc<dyn crabka_rebalancer::executor::phases::ClientFacade> = Arc::new(
        LiveClient::with_reassignment_request_timeout(client.clone(), reassignment_request_timeout),
    );

    spawn_recovery(
        state_topic.clone(),
        store.clone(),
        in_flight_slot.clone(),
        executor_state.clone(),
        live_client.clone(),
        shutdown.clone(),
        (
            arg_secs(args.state_load_timeout_secs),
            runtime_policy.recovery_load_poll_interval,
        ),
    );

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
            scrape_interval: arg_secs(args.metrics_scrape_interval_secs),
            retention: arg_secs(args.metrics_retention_secs),
        },
    ));

    let source = scraper_target_source(&args, snapshot.clone())?;

    let scraper = crabka_rebalancer::scraper::Scraper::new_with_http_timeout(
        source,
        arg_secs(args.metrics_scrape_interval_secs),
        usage_store.clone(),
        shutdown.clone(),
        runtime_policy.scraper_http_timeout,
    );
    tokio::spawn(scraper.run());

    let goal_registry = Arc::new(GoalRegistry::default_registry());

    let anomaly_store = Arc::new(crabka_rebalancer::detector::AnomalyStore::open(
        &args.data_dir,
        args.anomaly_ring_buffer_size,
    )?);

    let goal_ctx = GoalContext {
        imbalance_threshold: arg_percent(args.imbalance_threshold_pct),
        max_movements_per_proposal: args.max_movements_per_proposal,
        min_topic_leaders_per_broker: args.min_topic_leaders_per_broker,
        broker_capacities: broker_capacities.clone(),
        broker_usages: usage_store.clone(),
    };

    if detector_enabled(arg_secs(args.detector_tick_interval_secs)) {
        let detector_cfg = crabka_rebalancer::detector::DetectorConfig {
            tick_interval: arg_secs(args.detector_tick_interval_secs),
            broker_death_threshold: arg_secs(args.detector_broker_death_threshold_secs),
            under_replicated_threshold: arg_secs(args.detector_under_replicated_threshold_secs),
            disk_pressure_threshold: arg_fraction(args.detector_disk_pressure_pct),
            disk_critical_threshold: arg_fraction(args.detector_disk_critical_pct),
            slow_broker_multiplier: args.detector_slow_broker_multiplier,
            slow_broker_min_cores: args.detector_slow_broker_min_cores,
            default_mute_window: arg_secs(args.detector_mute_window_secs),
            auto_trigger_enabled: args.detector_auto_trigger_enabled,
            history_capacity: runtime_policy.detector_history_capacity.get(),
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
            crabka_rebalancer::detector::DetectorDependencies {
                usage_store: usage_store.clone(),
                capacities: broker_capacities.clone(),
                anomaly_store: anomaly_store.clone(),
                proposal_store: store.clone(),
                executor_state: executor_state.clone(),
                goal_registry: goal_registry.clone(),
                goal_ctx: goal_ctx.clone(),
                metrics: detector_metrics,
            },
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
        cancel_drain_timeout: runtime_policy.cancel_drain_timeout,
        cancel_drain_poll_interval: runtime_policy.cancel_drain_poll_interval,
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

    finish_shutdown(&in_flight_slot, ingester_handle, runtime_policy).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex as StdMutex, OnceLock};

    use crabka_units::convert::{ByteRateExt as _, RatioExt as _};

    use super::*;

    static ENV_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults =
            Args::try_parse_from(["crabka-rebalancer", "--bootstrap-servers", "127.0.0.1:9092"])
                .unwrap();
        assert2::assert!(defaults.client_dispatch_queue_capacity == 64);
        assert2::assert!(defaults.client_frame_max == crabka_units::mebibytes(100));

        let custom = Args::try_parse_from([
            "crabka-rebalancer",
            "--bootstrap-servers",
            "127.0.0.1:9092",
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
        ])
        .unwrap();
        assert2::assert!(custom.client_dispatch_queue_capacity == 7);
        assert2::assert!(custom.client_frame_max == crabka_units::kibibytes(32));

        for (option, invalid) in [
            ("--client-dispatch-queue-capacity", "0"),
            ("--client-frame-max", "101MiB"),
        ] {
            assert2::assert!(
                Args::try_parse_from([
                    "crabka-rebalancer",
                    "--bootstrap-servers",
                    "127.0.0.1:9092",
                    option,
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn runtime_policy_parses_overrides_and_rejects_invalid_relations() {
        let args = Args::try_parse_from([
            "crabka-rebalancer",
            "--bootstrap-servers",
            "127.0.0.1:9092",
            "--recovery-load-poll-interval",
            "37ms",
            "--state-fetch-max",
            "2MiB",
            "--state-produce-retry-attempts",
            "7",
            "--state-topic-min-cleanable-dirty-ratio",
            "2%",
        ])
        .unwrap();
        let policy = args.runtime.effective_policy().unwrap();
        assert2::assert!(policy.recovery_load_poll_interval == millis(37));
        assert2::assert!(policy.state_fetch_max == crabka_units::mebibytes(2));
        assert2::assert!(policy.state_produce_retry_attempts.get() == 7);
        assert2::assert!(policy.state_topic_min_cleanable_dirty_ratio == percent(2));

        let invalid = Args::try_parse_from([
            "crabka-rebalancer",
            "--bootstrap-servers",
            "127.0.0.1:9092",
            "--cancel-drain-timeout",
            "1s",
            "--cancel-drain-poll-interval",
            "1s",
        ])
        .unwrap();
        assert2::assert!(invalid.runtime.effective_policy().is_err());
    }

    #[test]
    fn runtime_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_REBALANCER_RUNTIME_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::runtime_policy_reads_environment_and_prefers_cli",
                ])
                .env(CHILD, "1")
                .env("CRABKA_REBALANCER_RECOVERY_LOAD_POLL_INTERVAL", "37ms")
                .status()
                .unwrap();
            assert2::assert!(status.success());
            return;
        }

        let from_env =
            Args::try_parse_from(["crabka-rebalancer", "--bootstrap-servers", "127.0.0.1:9092"])
                .unwrap();
        assert2::assert!(
            from_env
                .runtime
                .effective_policy()
                .unwrap()
                .recovery_load_poll_interval
                == millis(37)
        );
        let from_cli = Args::try_parse_from([
            "crabka-rebalancer",
            "--bootstrap-servers",
            "127.0.0.1:9092",
            "--recovery-load-poll-interval",
            "41ms",
        ])
        .unwrap();
        assert2::assert!(
            from_cli
                .runtime
                .effective_policy()
                .unwrap()
                .recovery_load_poll_interval
                == millis(41)
        );
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_REBALANCER_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("CRABKA_REBALANCER_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("CRABKA_REBALANCER_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env =
            Args::try_parse_from(["crabka-rebalancer", "--bootstrap-servers", "127.0.0.1:9092"])
                .unwrap();
        assert2::assert!(from_env.client_dispatch_queue_capacity == 7);
        assert2::assert!(from_env.client_frame_max == crabka_units::kibibytes(32));

        let from_cli = Args::try_parse_from([
            "crabka-rebalancer",
            "--bootstrap-servers",
            "127.0.0.1:9092",
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ])
        .unwrap();
        assert2::assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert2::assert!(from_cli.client_frame_max == crabka_units::kibibytes(64));
    }

    #[test]
    fn state_topic_load_warning_only_before_loaded() {
        for (_name, loaded, expected) in [("not loaded", false, true), ("loaded", true, false)] {
            assert2::assert!(should_warn_state_topic_load(loaded) == expected);
        }
    }

    #[test]
    fn recovery_load_wait_continues_only_before_loaded() {
        for (_name, loaded, expected) in [("not loaded", false, true), ("loaded", true, false)] {
            assert2::assert!(should_continue_recovery_load_wait(loaded) == expected);
        }
    }

    #[test]
    fn recovery_load_timeout_is_strictly_after_deadline() {
        let timeout = secs(5);
        assert2::assert!(!recovery_load_timed_out(secs(5), timeout));
        assert2::assert!(recovery_load_timed_out(secs(5) + millis(1), timeout));
    }

    #[test]
    fn detector_is_disabled_only_at_zero_interval() {
        for (_name, interval, expected) in
            [("disabled", Time::ZERO, false), ("enabled", secs(1), true)]
        {
            assert2::assert!(detector_enabled(interval) == expected);
        }
    }

    #[test]
    fn arg_secs_reads_the_flag_as_whole_seconds() {
        for (_name, value, expected) in [
            ("zero", 0, Time::ZERO),
            ("scrape default", 10, secs(10)),
            ("mute window default", 900, secs(900)),
            ("retention default", 43_200, secs(43_200)),
        ] {
            assert2::assert!(arg_secs(value) == expected);
        }
    }

    #[test]
    fn arg_secs_saturates_rather_than_wrapping() {
        assert2::assert!(arg_secs(u64::MAX) == Time::from_secs(i64::MAX));
    }

    #[test]
    fn arg_percent_reads_whole_percentages() {
        for (_name, value, expected) in [
            ("zero", 0, Ratio::ZERO),
            ("imbalance default", 10, fraction(0.1)),
            ("whole", 100, Ratio::ONE),
        ] {
            assert2::assert!(arg_percent(value) == expected);
        }
    }

    #[test]
    fn arg_fraction_reads_unit_interval_values() {
        for (_name, value, expected) in [
            ("disk pressure default", 0.85, percent(85)),
            ("disk critical default", 0.95, percent(95)),
        ] {
            assert2::assert!(arg_fraction(value) == expected);
        }
    }

    #[test]
    fn arg_bytes_per_sec_reads_the_throttle_flag() {
        for (_name, value, expected) in [
            ("unset", 0, ByteRate::ZERO),
            (
                "default throttle",
                50_000_000,
                crabka_units::bytes_per_sec(50_000_000),
            ),
        ] {
            assert2::assert!(arg_bytes_per_sec(value) == expected);
        }
    }

    #[test]
    fn reassignment_request_timeout_defaults_and_accepts_cli() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_var(
            "CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT",
            None::<&str>,
            || {
                let defaults = Args::try_parse_from([
                    "crabka-rebalancer",
                    "--bootstrap-servers",
                    "127.0.0.1:9092",
                ])
                .unwrap();
                assert2::assert!(defaults.reassignment_request_timeout == secs(60));
            },
        );

        let custom = Args::try_parse_from([
            "crabka-rebalancer",
            "--bootstrap-servers",
            "127.0.0.1:9092",
            "--reassignment-request-timeout",
            "37ms",
        ])
        .unwrap();
        assert2::assert!(custom.reassignment_request_timeout == millis(37));
    }

    #[test]
    fn reassignment_request_timeout_rejects_invalid_protocol_values() {
        assert2::assert!(
            Args::try_parse_from([
                "crabka-rebalancer",
                "--bootstrap-servers",
                "127.0.0.1:9092",
                "--reassignment-request-timeout",
                "0s",
            ])
            .is_err()
        );
        for value in ["0.5ms", "2147483648ms"] {
            let args = Args::try_parse_from([
                "crabka-rebalancer",
                "--bootstrap-servers",
                "127.0.0.1:9092",
                "--reassignment-request-timeout",
                value,
            ])
            .unwrap();
            assert2::assert!(
                crabka_rebalancer::executor::client_impl::ReassignmentRequestTimeout::new(
                    args.reassignment_request_timeout
                )
                .is_err()
            );
        }
    }

    #[test]
    fn reassignment_request_timeout_reads_environment() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_var(
            "CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT",
            Some("41ms"),
            || {
                let args = Args::try_parse_from([
                    "crabka-rebalancer",
                    "--bootstrap-servers",
                    "127.0.0.1:9092",
                ])
                .unwrap();
                assert2::assert!(args.reassignment_request_timeout == millis(41));
            },
        );
    }
}
