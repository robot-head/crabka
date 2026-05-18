//! `crabka-bench-report` — walk a directory of `RunOutput` JSON files and
//! emit a Markdown side-by-side comparison.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use crabka_bench_driver::report;

#[derive(Debug, Parser)]
#[command(name = "crabka-bench-report", version, about)]
struct Cli {
    /// Directory containing per-run `*.json` files.
    #[arg(long, default_value = "bench/results")]
    input_dir: PathBuf,
    /// Where to write the Markdown summary.
    #[arg(long, default_value = "bench/results/SUMMARY.md")]
    out: PathBuf,
    /// Fail on any unparseable JSON instead of skipping it.
    #[arg(long)]
    strict: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let md = report::render_markdown(&cli.input_dir, cli.strict)?;
    if let Some(parent) = cli.out.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(&cli.out, md)
        .with_context(|| format!("write summary to {}", cli.out.display()))?;
    println!("wrote {}", cli.out.display());
    Ok(())
}
