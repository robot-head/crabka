//! Crabka CLI.

use clap::{Parser, Subcommand};

mod format;
mod gres;
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
    /// Manage Chapter Gres tenants.
    Gres(gres::GresArgs),
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
        Command::Gres(args) => gres::run(args).await,
    };
    std::process::exit(rc);
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::*;

    #[test]
    fn gres_create_tenant_has_no_plain_password_flag() {
        let mut command = Cli::command();
        let help = command
            .find_subcommand_mut("gres")
            .and_then(|gres| gres.find_subcommand_mut("create-tenant"))
            .expect("gres create-tenant command")
            .render_long_help()
            .to_string();

        assert!(!help.contains("--password <"));
        assert!(help.contains("--password-file"));
        assert!(help.contains("--password-stdin"));
    }

    #[test]
    fn gres_password_sources_are_mutually_exclusive() {
        let parsed = Cli::try_parse_from([
            "crabka",
            "gres",
            "create-tenant",
            "--bootstrap",
            "127.0.0.1:9092",
            "--name",
            "tenant-a",
            "--user",
            "alice",
            "--password-file",
            "/tmp/pw",
            "--password-stdin",
        ]);

        assert!(parsed.is_err());
    }
}
