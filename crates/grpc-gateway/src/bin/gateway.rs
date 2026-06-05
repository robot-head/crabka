//! `crabka-grpc-gateway` binary entry point.
//!
//! Parses CLI flags, builds the Connect-RPC router and a minimal health
//! router, then serves both on the configured listen address.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tracing::info;

use tokio_util::sync::CancellationToken;

use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::dedup::DedupEngine;
use crabka_grpc_gateway::dedup::store::DedupStore;
use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
use crabka_grpc_gateway::health::{self, Readiness};
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::state::AppState;

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

    // Ensure the internal compacted dedup-claim topic exists before opening
    // any producer/consumer against it.
    ensure_dedup_topic(
        &config.bootstrap,
        &config.dedup_topic,
        config.dedup_partitions,
        config.dedup_window_ms,
        GatewayConfig::DEDUP_TOPIC_REPLICATION,
    )
    .await?;

    // Build the dedup store and run the ownership consumer in the background;
    // the gateway reports `/readyz` 503 until the store has warmed at least once.
    let store = Arc::new(DedupStore::new(config.dedup_partitions));
    let readiness = Readiness::new();
    let shutdown = CancellationToken::new();
    {
        let store = store.clone();
        let bootstrap = config.bootstrap.clone();
        let client_id = format!("{}-dedup-owner", config.client_id);
        let dedup_topic = config.dedup_topic.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = store
                .run_ownership(
                    bootstrap,
                    client_id,
                    dedup_topic,
                    "__crabka_grpc_gateway_dedup_owners".to_string(),
                    shutdown,
                )
                .await
            {
                tracing::error!(error = %e, "dedup ownership task exited with error");
            }
        });
    }
    {
        let store = store.clone();
        let readiness = readiness.clone();
        tokio::spawn(async move {
            loop {
                if store.has_warmed_once() {
                    readiness.set_ready();
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        });
    }

    let engine = Arc::new(DedupEngine::new(
        &config.bootstrap,
        &config.client_id,
        &config.dedup_txn_id_prefix,
        config.dedup_topic.clone(),
        config.dedup_partitions,
        store,
    ));
    let produce = ProduceCore::new(&config.bootstrap, &config.client_id, Arc::new(RawCodec))
        .await?
        .with_dedup(engine);
    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(config.clone()),
    });

    let app = crabka_grpc_gateway::router(state).merge(health::router(readiness));

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %listener.local_addr()?, "gateway listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown.cancel();
        })
        .await?;
    Ok(())
}
