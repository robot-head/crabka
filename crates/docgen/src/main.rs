use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "crabka-docgen", about = "Generate Crabka reference docs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write the full reference tree (operator + broker) under --out.
    All {
        #[arg(long)]
        out: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::All { out } => {
            crabka_docgen::emit::write_reference_tree(&out)?;
            eprintln!("wrote reference tree to {}", out.display());
            Ok(())
        }
    }
}
