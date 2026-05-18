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
use crabka_rebalancer::executor::state::InFlightFile;
use crabka_rebalancer::executor::{Execution, ExecutionHandle, ExecutorConfig, ExecutorState};
use crabka_rebalancer::goals::GoalContext;
use crabka_rebalancer::health::{HealthState, new_registry};
use crabka_rebalancer::ingest::{Ingester, new_shared_snapshot};
use crabka_rebalancer::metrics::RebalancerMetrics;
use crabka_rebalancer::model::proposal::ProposalStatus;
use crabka_rebalancer::model::store::ProposalStore;

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
    let executor_state = ExecutorState {
        store: store.clone(),
        config: executor_config,
        metrics: metrics.clone(),
        in_flight: in_flight_slot.clone(),
    };

    let live_client: Arc<dyn crabka_rebalancer::executor::phases::ClientFacade> =
        Arc::new(LiveClient::new(client.clone()));

    // Recovery on startup: replay in_flight.json if present.
    if let Some(in_flight) = InFlightFile::load(&args.data_dir)? {
        info!(
            proposal_id = %in_flight.proposal_id,
            phase = ?in_flight.phase,
            "recovering in-flight execution"
        );
        if let Some(proposal) = store.get(&in_flight.proposal_id) {
            let prop_for_resume = store
                .mutate(&in_flight.proposal_id, |p| {
                    p.status = ProposalStatus::Executing;
                })
                .unwrap_or(proposal);
            let cancel = CancellationToken::new();
            let handle_cancel = cancel.clone();
            let exec_state = executor_state.clone();
            let exec_client = live_client.clone();
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
                "in_flight.json references unknown proposal; clearing"
            );
            let _ = InFlightFile::delete(&args.data_dir);
        }
    }

    // Load broker capacity config (optional).
    let broker_capacities = if args.broker_capacity_file.is_empty() {
        std::sync::Arc::new(crabka_rebalancer::capacity::BrokerCapacities::default())
    } else {
        match crabka_rebalancer::capacity::load::load_from_path(std::path::Path::new(
            &args.broker_capacity_file,
        )) {
            Ok(c) => {
                info!(
                    path = %args.broker_capacity_file,
                    broker_count = c.by_broker.len(),
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

    let app_state = Arc::new(AppState {
        snapshot: snapshot.clone(),
        store,
        goal_registry: GoalRegistry::default_registry(),
        goal_ctx: GoalContext {
            imbalance_threshold_pct: args.imbalance_threshold_pct,
            max_movements_per_proposal: args.max_movements_per_proposal,
            min_topic_leaders_per_broker: args.min_topic_leaders_per_broker,
            broker_capacities: broker_capacities.clone(),
        },
        metrics: metrics.clone(),
        executor: executor_state,
        client_facade: live_client,
    });

    let connect_router = crabka_rebalancer::api::router(app_state);
    let health_router = crabka_rebalancer::health::router(HealthState {
        snapshot: snapshot.clone(),
        registry,
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
