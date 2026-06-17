use clap::Parser;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

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

    let _cli = Cli::parse();
    tracing::info!("crabka-replicator starting");
    Ok(())
}
