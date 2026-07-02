// `#[tracing::instrument]` on the deeply-nested replication futures pushes the
// type-layout query past the default depth limit; raise it (mirrors lib.rs).
#![recursion_limit = "256"]

use anyhow::Context as _;
use clap::Parser;
use crabka_replicator::{config::ReplicatorConfig, supervisor::FlowSupervisor};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[derive(Debug, Parser)]
#[command(name = "crabka-replicator", version, about)]
struct Cli {
    /// Path to the replicator YAML config.
    #[arg(long, env = "CRABKA_REPLICATOR_CONFIG")]
    config: std::path::PathBuf,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "crabka_replicator=info,info".into());
    tracing_subscriber::registry()
        .with(crabka_logfmt::layer(filter, std::io::stdout))
        .init();

    let cli = Cli::parse();
    let yaml = std::fs::read_to_string(&cli.config)
        .with_context(|| format!("reading config {}", cli.config.display()))?;
    let config = ReplicatorConfig::from_yaml(&yaml)?;
    config.validate()?;

    let supervisor = FlowSupervisor::run(config).await?;
    tracing::info!("crabka-replicator running; send SIGINT/ctrl-c to stop");
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested; draining flows");
    supervisor.shutdown().await;
    Ok(())
}
