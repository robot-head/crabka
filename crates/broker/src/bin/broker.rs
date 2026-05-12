//! `crabka-broker` — single-node Kafka-compatible broker daemon.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

use crabka_broker::{Broker, BrokerConfig};
use crabka_log::LogConfig;

#[derive(Debug, Parser)]
#[command(
    name = "crabka-broker",
    version,
    about = "Single-node Kafka-compatible broker (MVP)"
)]
struct Args {
    /// TCP address to listen on.
    #[arg(long, default_value = "127.0.0.1:9092")]
    listen_addr: SocketAddr,

    /// `host:port` to advertise to clients (defaults to `listen_addr`).
    #[arg(long)]
    advertised_listener: Option<String>,

    /// Directory containing per-partition log dirs.
    #[arg(long, default_value = "./crabka-data")]
    log_dir: PathBuf,

    /// Numeric broker id.
    #[arg(long, default_value_t = 1)]
    broker_id: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_broker=info,crabka_log=info,info".into()),
        )
        .init();

    let args = Args::parse();
    let advertised = args
        .advertised_listener
        .unwrap_or_else(|| args.listen_addr.to_string());
    let controller_addr: std::net::SocketAddr = {
        let mut a = args.listen_addr;
        a.set_port(9093);
        a
    };
    let node_id = args.broker_id as u64;
    let config = BrokerConfig {
        broker_id: args.broker_id,
        listen_addr: args.listen_addr,
        advertised_listener: advertised,
        log_dir: args.log_dir,
        log_config: LogConfig::default(),
        node_id,
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(node_id, controller_addr)],
    };

    let handle = Broker::start(config).await?;
    tracing::info!(addr = %handle.listen_addr(), "crabka-broker listening");

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    handle.shutdown().await;
    tracing::info!("crabka-broker stopped");
    Ok(())
}
