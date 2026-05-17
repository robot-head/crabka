use clap::{Parser, Subcommand};

use crabka_operator::config::OperatorConfig;
use crabka_operator::{gen_crds, run};

#[derive(Debug, Parser)]
#[command(name = "crabka-operator", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the operator: watch CRDs and reconcile.
    Run(RunArgs),
    /// Emit CRD YAML manifests to a directory.
    GenCrds { out_dir: std::path::PathBuf },
}

#[derive(Debug, clap::Args)]
struct RunArgs {
    #[command(flatten)]
    config: OperatorConfig,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // rustls 0.23 refuses to auto-pick a CryptoProvider when multiple
    // are linkable (or none is enabled at the binary level). kube's
    // rustls-tls feature pulls rustls transitively without selecting
    // one, so install ring explicitly before any TLS use.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install default rustls CryptoProvider");

    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run::run(args.config).await,
        Command::GenCrds { out_dir } => gen_crds::write_all(&out_dir),
    }
}
