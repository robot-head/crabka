use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "crabka-docgen", about = "Generate Crabka reference docs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write the full reference tree for the operator and the broker under --out.
    All {
        #[arg(long)]
        out: PathBuf,
    },
    /// Sync fenced code blocks in website markdown from anchored source regions.
    Snippets {
        /// Website content dir to scan (default: website/content).
        #[arg(long, default_value = "website/content")]
        content: std::path::PathBuf,
        /// Crates dir that snippet paths are relative to (default: crates).
        #[arg(long, default_value = "crates")]
        crates: std::path::PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::All { out } => {
            crabka_docgen::emit::write_reference_tree(&out)?;
            eprintln!("wrote reference tree to {}", out.display());
            Ok(())
        }
        Command::Snippets { content, crates } => {
            let n = crabka_docgen::sync_snippets(&content, &crates)?;
            eprintln!("synced snippets in {n} file(s)");
            Ok(())
        }
    }
}
