//! crabka-schema-registry: Confluent Schema Registry-compatible REST service.

use std::net::SocketAddr;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crabka_schema_registry::config::RegistryConfig;
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::{self, AppState};

#[derive(Debug, Parser)]
#[command(
    name = "crabka-schema-registry",
    version,
    about = "Confluent Schema Registry-compatible service for Crabka"
)]
struct Args {
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_LISTEN_ADDR",
        default_value = "0.0.0.0:8081"
    )]
    listen_addr: SocketAddr,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_SCHEMAS_TOPIC",
        default_value = "_schemas"
    )]
    schemas_topic: String,
    #[arg(long, env = "SCHEMA_REGISTRY_SCHEMAS_TOPIC_RF", default_value_t = 3)]
    schemas_topic_rf: i32,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_CLIENT_ID",
        default_value = "crabka-schema-registry"
    )]
    client_id: String,
    #[arg(long, env = "SCHEMA_REGISTRY_ADVERTISED_URL")]
    advertised_url: Option<String>,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_GROUP_ID",
        default_value = "schema-registry"
    )]
    group_id: String,
    #[arg(
        long,
        env = "SCHEMA_REGISTRY_LEADER_ELIGIBILITY",
        default_value_t = true
    )]
    leader_eligibility: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_schema_registry=info,info".into()),
        )
        .init();

    let args = Args::parse();
    let cfg = RegistryConfig {
        bootstrap: args.bootstrap_servers,
        schemas_topic: args.schemas_topic,
        schemas_topic_rf: args.schemas_topic_rf,
        client_id: args.client_id,
        advertised_url: args
            .advertised_url
            .clone()
            .unwrap_or_else(|| format!("http://{}", args.listen_addr)),
        group_id: args.group_id.clone(),
        leader_eligibility: args.leader_eligibility,
    };
    info!(
        listen = %args.listen_addr,
        bootstrap = %cfg.bootstrap,
        topic = %cfg.schemas_topic,
        "crabka-schema-registry starting"
    );

    let shutdown = CancellationToken::new();
    let store = KafkaStore::start(&cfg, shutdown.clone()).await?;
    let primary = crabka_schema_registry::election::Election::start(&cfg, shutdown.clone()).await?;
    let fwd = rest::forward::ForwardState {
        primary,
        http: reqwest::Client::new(),
        node_id: cfg.advertised_url.clone(),
    };
    let app = rest::router_with_forwarding(AppState { store }, fwd);

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
