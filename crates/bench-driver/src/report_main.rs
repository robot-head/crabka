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
    /// Also write a self-contained Plotly HTML report (bar charts with
    /// run-to-run error bars + averaged time-series line charts) here.
    #[arg(long)]
    html: Option<PathBuf>,
    /// Also write the website HTML fragment (per-run + averaged throughput /
    /// CPU / memory charts) here — embedded by the Zola `benchmark_charts`
    /// shortcode. Typically `website/static/benchmarks/charts.html`.
    #[arg(long)]
    web_fragment: Option<PathBuf>,
    /// Title for the HTML report.
    #[arg(long, default_value = "Crabka vs Strimzi benchmark")]
    title: String,
    /// Fail on any unparseable JSON instead of skipping it.
    #[arg(long)]
    strict: bool,
    /// Exit non-zero unless every failover cell proves Crabka recovered no
    /// slower than Kafka, with rate, drop, latency-spike, and topology evidence.
    #[arg(long)]
    failover_gate: bool,
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

    if let Some(p) = &cli.html {
        let html = report::render_html(&cli.input_dir, cli.strict, &cli.title)?;
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(p, html).with_context(|| format!("write html to {}", p.display()))?;
        println!("wrote {}", p.display());
    }

    if let Some(p) = &cli.web_fragment {
        let frag = report::render_web_fragment(&cli.input_dir, cli.strict)?;
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(p, frag)
            .with_context(|| format!("write web fragment to {}", p.display()))?;
        println!("wrote {}", p.display());
    }

    if cli.failover_gate {
        let violations = report::failover_gate_violations(&cli.input_dir, cli.strict)?;
        if violations.is_empty() {
            println!("failover gate: PASS");
        } else {
            for violation in &violations {
                eprintln!("failover gate: {violation}");
            }
            anyhow::bail!(
                "failover gate failed with {} violation(s)",
                violations.len()
            );
        }
    }

    Ok(())
}
