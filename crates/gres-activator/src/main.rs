use std::{net::SocketAddr, sync::Arc, time::Duration};

use clap::Parser;
use crabka_gres_activator::{
    ActivatorConfig, ControlRegistryWakeRegistry, WakeCoordinator, serve_conn,
};
use crabka_gres_control::Registry;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(name = "crabka-gres-activator")]
struct Args {
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    bootstrap: String,
    #[arg(long, default_value_t = 250)]
    registry_poll_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    cold_start_timeout_ms: u64,
    #[arg(long, default_value = "{tenant}:5432")]
    backend_endpoint_template: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let cfg = ActivatorConfig {
        listen: args.listen,
        bootstrap: args.bootstrap,
        registry_poll: Duration::from_millis(args.registry_poll_ms),
        cold_start_timeout: Duration::from_millis(args.cold_start_timeout_ms),
        backend_endpoint_template: args.backend_endpoint_template,
    };
    let mut registry = Registry::connect(&cfg.bootstrap).await?;
    registry.ensure_topic(1).await?;
    let coordinator = Arc::new(WakeCoordinator::new(ControlRegistryWakeRegistry::new(
        registry,
        cfg.clone(),
    )));
    let listener = TcpListener::bind(cfg.listen).await?;
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let coordinator = Arc::clone(&coordinator);
        let cfg = cfg.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_conn(stream, &coordinator, &cfg).await {
                tracing::warn!(%peer_addr, %error, "gres activator connection failed");
            }
        });
    }
}
