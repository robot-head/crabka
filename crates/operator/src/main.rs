use clap::{Parser, Subcommand};

use crabka_operator::config::OperatorConfig;

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
    /// Emit CRD YAML manifests to a directory (for committing under deploy/crds/).
    GenCrds { out_dir: std::path::PathBuf },
}

#[derive(Debug, clap::Args)]
struct RunArgs {
    #[command(flatten)]
    config: OperatorConfig,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(_) => anyhow::bail!("`run` not implemented yet (Task 9)"),
        Command::GenCrds { .. } => anyhow::bail!("`gen-crds` not implemented yet (Task 4)"),
    }
}
