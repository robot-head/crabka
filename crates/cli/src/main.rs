//! Crabka CLI. Only the `format` subcommand exists.

use clap::{Parser, Subcommand};

mod format;
mod ids;

#[derive(Parser)]
#[command(name = "crabka", version, about = "Crabka operator CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Format a fresh log directory, optionally seeding SCRAM credentials.
    Format(format::FormatArgs),
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let rc = match cli.command {
        Command::Format(args) => format::run(args).await,
    };
    std::process::exit(rc);
}
