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
use crabka_grpc_gateway::dedup::membership::{MembershipPublisher, MembershipStore};
use crabka_grpc_gateway::dedup::store::DedupStore;
use crabka_grpc_gateway::dedup::topic::{ensure_dedup_topic, ensure_membership_topic};
use crabka_grpc_gateway::forward::{self, Forwarder};
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
        default_value = "__crabka_grpc_dedup"
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

    /// Address peers reach this gateway at (e.g. `gw-0.gw:9500`). Required for
    /// active-active forwarding; must be routable from other replicas.
    #[arg(long, env = "CRABKA_GATEWAY_ADVERTISED_ADDR")]
    advertised_addr: String,

    /// Internal membership / owner-routing topic.
    #[arg(
        long,
        env = "CRABKA_GATEWAY_MEMBERSHIP_TOPIC",
        default_value = "__crabka_grpc_gateway_membership"
    )]
    membership_topic: String,
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
        advertised_addr: args.advertised_addr.clone(),
        membership_topic: args.membership_topic.clone(),
        tls: None, // wired in Task 5
    };

    run(config).await
}

async fn run(config: GatewayConfig) -> anyhow::Result<()> {
    // Ensure internal topics exist before opening any producer/consumer.
    ensure_dedup_topic(
        &config.bootstrap,
        &config.dedup_topic,
        config.dedup_partitions,
        config.dedup_window_ms,
        GatewayConfig::DEDUP_TOPIC_REPLICATION,
    )
    .await?;
    ensure_membership_topic(
        &config.bootstrap,
        &config.membership_topic,
        GatewayConfig::MEMBERSHIP_TOPIC_REPLICATION,
    )
    .await?;

    let node_id = uuid::Uuid::new_v4().to_string();
    let store = Arc::new(DedupStore::new(config.dedup_partitions));
    let readiness = Readiness::new();
    let shutdown = CancellationToken::new();

    // Membership: tail the routing table, and install the publisher BEFORE the
    // ownership consumer starts so its first assignment is published.
    let membership = Arc::new(MembershipStore::new());
    spawn_membership_reader(&config, &membership, &node_id, &shutdown);
    let publisher = Arc::new(
        MembershipPublisher::new(
            &config.bootstrap,
            &format!("{}-membership-pub", config.client_id),
            node_id.clone(),
            config.advertised_addr.clone(),
            config.membership_topic.clone(),
        )
        .await?,
    );
    store.set_membership(publisher);

    spawn_ownership_consumer(&config, &store, &shutdown);
    spawn_readiness_watcher(store.clone(), readiness.clone());

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
        .with_dedup(engine)
        .with_forwarding(
            membership,
            Arc::new(Forwarder::new()),
            config.advertised_addr.clone(),
        );
    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(config.clone()),
    });

    let app = crabka_grpc_gateway::router(state.clone())
        .merge(health::router(readiness))
        .merge(forward::forward_router(state.clone()));

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

fn spawn_membership_reader(
    config: &GatewayConfig,
    membership: &Arc<MembershipStore>,
    node_id: &str,
    shutdown: &CancellationToken,
) {
    let membership = membership.clone();
    let bootstrap = config.bootstrap.clone();
    let client_id = format!("{}-membership", config.client_id);
    let topic = config.membership_topic.clone();
    let group = format!("__crabka_grpc_gateway_membership_reader-{node_id}");
    let shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Err(e) = membership
            .run_membership(bootstrap, client_id, topic, group, shutdown)
            .await
        {
            tracing::error!(error = %e, "membership reader exited with error");
        }
    });
}

fn spawn_ownership_consumer(
    config: &GatewayConfig,
    store: &Arc<DedupStore>,
    shutdown: &CancellationToken,
) {
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

fn spawn_readiness_watcher(store: Arc<DedupStore>, readiness: Readiness) {
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
