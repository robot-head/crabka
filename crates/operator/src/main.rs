use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "crabka-operator", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the operator: watch CRDs and reconcile.
    Run,
    /// Emit CRD YAML manifests to a directory (for committing under deploy/crds/).
    GenCrds {
        /// Output directory.
        out_dir: std::path::PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run => {
            anyhow::bail!("`run` not implemented yet (Task 9)");
        }
        Command::GenCrds { out_dir: _ } => {
            anyhow::bail!("`gen-crds` not implemented yet (Task 4)");
        }
    }
}
