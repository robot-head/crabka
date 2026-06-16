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
    /// Also write a wide per-run summary CSV (one row per run) here.
    #[arg(long)]
    csv: Option<PathBuf>,
    /// Also write a long-format time-series CSV (one row per run × time-offset
    /// × metric) here — the graph-ready export for values over the test.
    #[arg(long)]
    timeseries_csv: Option<PathBuf>,
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

    if let Some(p) = &cli.csv {
        let csv = report::render_csv(&cli.input_dir, cli.strict)?;
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(p, csv).with_context(|| format!("write csv to {}", p.display()))?;
        println!("wrote {}", p.display());
    }

    if let Some(p) = &cli.timeseries_csv {
        let csv = report::render_timeseries_csv(&cli.input_dir, cli.strict)?;
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(p, csv)
            .with_context(|| format!("write timeseries csv to {}", p.display()))?;
        println!("wrote {}", p.display());
    }

    Ok(())
}
