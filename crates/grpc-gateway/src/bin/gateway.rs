//! `crabka-grpc-gateway` binary entry point.
//!
//! Parses CLI flags, builds the Connect-RPC router and a minimal health
//! router, then serves both on the configured listen address.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tracing::info;

use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::state::AppState;

// ── minimal health / readiness ────────────────────────────────────────────────

#[derive(Clone, Default)]
struct Readiness(Arc<std::sync::atomic::AtomicBool>);

impl Readiness {
    fn new() -> Self {
        Self(Arc::new(std::sync::atomic::AtomicBool::new(false)))
    }

    fn set_ready(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn is_ready(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

mod health {
    use super::Readiness;
    use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};

    pub fn router(r: Readiness) -> Router {
        Router::new()
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .with_state(r)
    }

    async fn healthz() -> impl IntoResponse {
        (StatusCode::OK, "ok")
    }

    async fn readyz(State(r): State<Readiness>) -> impl IntoResponse {
        if r.is_ready() {
            (StatusCode::OK, "ready")
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        }
    }
}

// ── CLI ────────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(
    name = "crabka-grpc-gateway",
    version,
    about = "gRPC / Connect-RPC + HTTP gateway into Crabka topics"
)]
struct Args {
    /// `host:port,host:port,...` bootstrap brokers.
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,

    /// Bind address for the Connect-RPC + health server.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_LISTEN_ADDR",
        default_value = "0.0.0.0:9500"
    )]
    listen_addr: SocketAddr,

    /// `client.id` for the native clients this gateway opens.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_CLIENT_ID",
        default_value = "crabka-grpc-gateway"
    )]
    client_id: String,

    /// Internal dedup topic name (used by Task 12).
    #[arg(
        long,
        env = "CRABKA_GATEWAY_DEDUP_TOPIC",
        default_value = "__crabka_gateway_dedup"
    )]
    dedup_topic: String,

    /// Dedup topic partition count.
    #[arg(long, env = "CRABKA_GATEWAY_DEDUP_PARTITIONS", default_value_t = 8)]
    dedup_partitions: u32,

    /// Dedup window (ms).
    #[arg(
        long,
        env = "CRABKA_GATEWAY_DEDUP_WINDOW_MS",
        default_value_t = 3_600_000
    )]
    dedup_window_ms: i64,

    /// Transactional id prefix for the dedup path (Task 12).
    #[arg(
        long,
        env = "CRABKA_GATEWAY_DEDUP_TXN_PREFIX",
        default_value = "crabka-gw-dedup"
    )]
    dedup_txn_id_prefix: String,
}

// ── main ───────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_grpc_gateway=info,info".into()),
        )
        .init();

    let args = Args::parse();
    info!(
        listen = %args.listen_addr,
        bootstrap = %args.bootstrap_servers,
        "crabka-grpc-gateway starting"
    );

    let config = GatewayConfig {
        bootstrap: args.bootstrap_servers.clone(),
        listen_addr: args.listen_addr,
        client_id: args.client_id.clone(),
        dedup_topic: args.dedup_topic.clone(),
        dedup_partitions: args.dedup_partitions,
        dedup_window_ms: args.dedup_window_ms,
        dedup_txn_id_prefix: args.dedup_txn_id_prefix.clone(),
    };

    let produce =
        ProduceCore::new(&config.bootstrap, &config.client_id, Arc::new(RawCodec)).await?;
    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(config.clone()),
    });

    let readiness = Readiness::new();
    readiness.set_ready(); // dedup wired in Task 12

    let app = crabka_grpc_gateway::router(state).merge(health::router(readiness.clone()));

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %listener.local_addr()?, "gateway listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
