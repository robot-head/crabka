//! `crabka-rebalancer` — Cruise-Control-equivalent partition
//! rebalancer for Crabka clusters. Slice 43a: advisor surface only —
//! propose / dry-run / list / get. Execute lands in slice 43b.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crabka_rebalancer::api::GoalRegistry;
use crabka_rebalancer::api::handlers::AppState;
use crabka_rebalancer::goals::GoalContext;
use crabka_rebalancer::health::{HealthState, new_registry};
use crabka_rebalancer::ingest::{Ingester, new_shared_snapshot};
use crabka_rebalancer::model::ProposalStore;

#[derive(Debug, Parser)]
#[command(
    name = "crabka-rebalancer",
    version,
    about = "Cruise-Control-equivalent partition rebalancer (advisor, slice 43a)"
)]
struct Args {
    /// `host:port,host:port,...` of brokers to use for bootstrap.
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,

    /// Bind address for the Connect-RPC + operational HTTP server.
    #[arg(long, env = "CRABKA_REBALANCER_LISTEN_ADDR", default_value = "0.0.0.0:9300")]
    listen_addr: SocketAddr,

    /// Cluster-state snapshot cadence.
    #[arg(long, env = "CRABKA_SCRAPE_INTERVAL_SECS", default_value_t = 10)]
    scrape_interval_secs: u64,

    /// `(max - min) * 100 / total` must exceed this for soft goals to act.
    #[arg(long, env = "CRABKA_IMBALANCE_THRESHOLD_PCT", default_value_t = 10)]
    imbalance_threshold_pct: u32,

    /// Safety cap on the total number of movements per proposal.
    #[arg(long, env = "CRABKA_MAX_MOVEMENTS_PER_PROPOSAL", default_value_t = 256)]
    max_movements_per_proposal: usize,

    /// In-memory ring buffer capacity for recent proposals.
    #[arg(long, env = "CRABKA_PROPOSAL_RING_BUFFER_SIZE", default_value_t = 20)]
    proposal_ring_buffer_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_rebalancer=info,info".into()),
        )
        .init();

    let args = Args::parse();
    info!(listen = %args.listen_addr, bootstrap = %args.bootstrap_servers, "crabka-rebalancer starting");

    // Admin client.
    let client = crabka_client_core::Client::builder()
        .bootstrap(args.bootstrap_servers.clone())
        .client_id("crabka-rebalancer")
        .build()
        .await?;

    // Shared snapshot state.
    let snapshot = new_shared_snapshot();

    // Ingester.
    let shutdown = CancellationToken::new();
    let ingester = Ingester::new(
        client.clone(),
        Duration::from_secs(args.scrape_interval_secs),
        snapshot.clone(),
        shutdown.clone(),
    );
    tokio::spawn(ingester.run());

    // Service state.
    let registry = Arc::new(Mutex::new(new_registry()));
    let store = Arc::new(ProposalStore::new(args.proposal_ring_buffer_size));
    let app_state = Arc::new(AppState {
        snapshot: snapshot.clone(),
        store,
        goal_registry: GoalRegistry::default_registry(),
        goal_ctx: GoalContext {
            imbalance_threshold_pct: args.imbalance_threshold_pct,
            max_movements_per_proposal: args.max_movements_per_proposal,
        },
    });
    let connect_router = crabka_rebalancer::api::router(app_state);

    let health_router = crabka_rebalancer::health::router(HealthState {
        snapshot: snapshot.clone(),
        registry,
    });

    // Merge Connect + health onto one axum app.
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
    Ok(())
}
