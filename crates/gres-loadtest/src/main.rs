//! CLI for the crabka-gres scalability and fault-injection harness.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use crabka_gres_loadtest::{
    cluster::Binaries,
    report::{self, LatencySummary, RunReport},
    runner::{self, RunConfig},
    scenario::{ModeSpec, Scenario},
};
use tracing_subscriber::EnvFilter;

/// `max_offset_ms` for the HLC leg of `compare` when the scenario's own
/// mode is not HLC.
const DEFAULT_HLC_MAX_OFFSET_MS: u64 = 250;

#[derive(Parser)]
#[command(
    name = "crabka-gres-loadtest",
    about = "Scenario-driven scalability and fault-injection harness for crabka-gres"
)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Run one scenario and write its JSON + Markdown report.
    Run {
        /// Path to the scenario YAML.
        #[arg(long)]
        scenario: PathBuf,
        /// Override the scenario's timestamp-source mode
        /// (`logical-tso` or `hlc`).
        #[arg(long)]
        mode: Option<String>,
        /// `max_offset_ms` when `--mode hlc` is given.
        #[arg(long, default_value_t = 250)]
        hlc_max_offset_ms: u64,
        /// Output directory for reports.
        #[arg(long, default_value = "loadtest-out")]
        out: PathBuf,
        /// Keep the cluster work dir (data + logs) after a successful run.
        #[arg(long)]
        keep_work_dir: bool,
    },
    /// Run one scenario under both timestamp modes and render a comparison.
    Compare {
        /// Path to the scenario YAML.
        #[arg(long)]
        scenario: PathBuf,
        /// Output directory for reports.
        #[arg(long, default_value = "loadtest-out")]
        out: PathBuf,
        /// Keep the cluster work dirs (data + logs) after successful runs.
        #[arg(long)]
        keep_work_dir: bool,
    },
    /// Parse and validate a scenario without running it.
    Validate {
        /// Path to the scenario YAML.
        #[arg(long)]
        scenario: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();
    match Cli::parse().command {
        CliCommand::Run {
            scenario,
            mode,
            hlc_max_offset_ms,
            out,
            keep_work_dir,
        } => {
            run(
                &scenario,
                mode.as_deref(),
                hlc_max_offset_ms,
                &out,
                keep_work_dir,
            )
            .await
        }
        CliCommand::Compare {
            scenario,
            out,
            keep_work_dir,
        } => compare(&scenario, &out, keep_work_dir).await,
        CliCommand::Validate { scenario } => validate(&scenario),
    }
}

/// The `run` subcommand: one scenario, optional mode override.
async fn run(
    scenario_path: &Path,
    mode: Option<&str>,
    hlc_max_offset_ms: u64,
    out: &Path,
    keep_work_dir: bool,
) -> anyhow::Result<()> {
    let scenario = load_scenario(scenario_path)?;
    let mode_override = parse_mode(mode, hlc_max_offset_ms)?;
    let effective_mode = mode_override.unwrap_or(scenario.mode);
    let binaries = Binaries::resolve()?;
    let report = runner::run_scenario(RunConfig {
        scenario,
        mode_override,
        out_dir: out.to_path_buf(),
        binaries,
        keep_work_dir,
    })
    .await?;
    print_summary(&report, out, effective_mode);
    Ok(())
}

/// The `compare` subcommand: the same scenario under `logical-tso` then
/// `hlc`, plus a side-by-side comparison report.
async fn compare(scenario_path: &Path, out: &Path, keep_work_dir: bool) -> anyhow::Result<()> {
    let scenario = load_scenario(scenario_path)?;
    let binaries = Binaries::resolve()?;
    let hlc = match scenario.mode {
        hlc @ ModeSpec::Hlc { .. } => hlc,
        ModeSpec::LogicalTso => ModeSpec::Hlc {
            max_offset_ms: DEFAULT_HLC_MAX_OFFSET_MS,
        },
    };
    let mut reports = Vec::with_capacity(2);
    for mode in [ModeSpec::LogicalTso, hlc] {
        let report = runner::run_scenario(RunConfig {
            scenario: scenario.clone(),
            mode_override: Some(mode),
            out_dir: out.to_path_buf(),
            binaries: binaries.clone(),
            keep_work_dir,
        })
        .await
        .with_context(|| format!("run scenario {} under {mode}", scenario.name))?;
        print_summary(&report, out, mode);
        reports.push(report);
    }
    let (left, right) = (&reports[0], &reports[1]);
    let comparison_path = out.join(format!("{}-comparison.md", scenario.name));
    std::fs::write(&comparison_path, report::render_comparison(left, right))
        .with_context(|| format!("write {}", comparison_path.display()))?;
    println!();
    println!("{}", delta_headline(left, right));
    println!("comparison: {}", comparison_path.display());
    Ok(())
}

/// The `validate` subcommand: parse + validate, report `ok` or fail.
fn validate(scenario_path: &Path) -> anyhow::Result<()> {
    let scenario = load_scenario(scenario_path)?;
    println!("ok: {}", scenario.name);
    Ok(())
}

/// Loads and validates a scenario, naming the path in the error.
fn load_scenario(path: &Path) -> anyhow::Result<Scenario> {
    Scenario::from_yaml_file(path).with_context(|| format!("load scenario {}", path.display()))
}

/// Maps the `--mode` string to a mode override.
fn parse_mode(mode: Option<&str>, hlc_max_offset_ms: u64) -> anyhow::Result<Option<ModeSpec>> {
    match mode {
        None => Ok(None),
        Some("logical-tso") => Ok(Some(ModeSpec::LogicalTso)),
        Some("hlc") => Ok(Some(ModeSpec::Hlc {
            max_offset_ms: hlc_max_offset_ms,
        })),
        Some(other) => anyhow::bail!("unknown mode {other:?}: expected `logical-tso` or `hlc`"),
    }
}

/// Prints the short human summary of one run to stdout.
fn print_summary(report: &RunReport, out_dir: &Path, mode: ModeSpec) {
    let paths = runner::report_paths(out_dir, &report.scenario, mode);
    println!("scenario:  {} ({})", report.scenario, report.mode);
    println!(
        "committed: {} txn, {:.2} tps mean, {} failed",
        report.throughput.committed_txn, report.throughput.tps_mean, report.throughput.failed_txn
    );
    match busiest_class(report) {
        Some((class, latency)) => println!(
            "p99:       {:.2} ms ({class}, {} ops)",
            latency.p99_ms, latency.count
        ),
        None => println!("p99:       no operations completed"),
    }
    println!(
        "errors:    {} serialization retries, {} unavailable, {} connection, {} other",
        report.errors.serialization_retries,
        report.errors.unavailable,
        report.errors.connection_errors,
        report.errors.other
    );
    println!("reports:   {}", paths.json.display());
    println!("           {}", paths.markdown.display());
}

/// The latency class with the most completed operations, if any.
fn busiest_class(report: &RunReport) -> Option<(&str, &LatencySummary)> {
    report
        .latency_by_class
        .iter()
        .max_by_key(|(_, summary)| summary.count)
        .map(|(class, summary)| (class.as_str(), summary))
}

/// One-line mean-TPS delta between the two compared runs.
fn delta_headline(left: &RunReport, right: &RunReport) -> String {
    let left_tps = left.throughput.tps_mean;
    let right_tps = right.throughput.tps_mean;
    let delta = if left_tps > 0.0 {
        format!("{:+.2}%", (right_tps - left_tps) / left_tps * 100.0)
    } else {
        "n/a".to_owned()
    };
    format!(
        "mean tps: {left_tps:.2} ({}) vs {right_tps:.2} ({}) — {delta}",
        left.mode, right.mode
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use crabka_gres_loadtest::report::{
        EfficiencySummary, ErrorSummary, ThroughputSummary, TopologySummary,
    };

    use super::*;

    fn fixture(mode: &str, tps_mean: f64, classes: &[(&str, u64, f64)]) -> RunReport {
        RunReport {
            scenario: "steady".to_owned(),
            description: String::new(),
            mode: mode.to_owned(),
            started_unix_ms: 0,
            topology: TopologySummary {
                nodes: 2,
                ranges: 3,
            },
            duration_s: 60.0,
            throughput: ThroughputSummary {
                committed_txn: 100,
                failed_txn: 0,
                tps_mean,
            },
            latency_by_class: classes
                .iter()
                .map(|(class, count, p99_ms)| {
                    (
                        (*class).to_owned(),
                        LatencySummary {
                            count: *count,
                            mean_ms: 1.0,
                            p50_ms: 1.0,
                            p95_ms: 2.0,
                            p99_ms: *p99_ms,
                            p999_ms: 5.0,
                            max_ms: 9.0,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            errors: ErrorSummary::default(),
            timeline: Vec::new(),
            resources: Vec::new(),
            efficiency: EfficiencySummary {
                total_cpu_core_seconds: 1.0,
                committed_txn_per_cpu_second: 100.0,
            },
            faults: Vec::new(),
        }
    }

    #[test]
    fn parse_mode_maps_strings_to_mode_specs() {
        let cases = [
            (None, 250, Some(None)),
            (Some("logical-tso"), 250, Some(Some(ModeSpec::LogicalTso))),
            (
                Some("hlc"),
                300,
                Some(Some(ModeSpec::Hlc { max_offset_ms: 300 })),
            ),
            (Some("banana"), 250, None),
        ];
        for (input, max_offset, expected) in cases {
            if let Some(expected) = expected {
                let parsed = parse_mode(input, max_offset).expect("valid mode");
                assert!(parsed == expected, "input {input:?}");
            } else {
                assert!(let Err(_) = parse_mode(input, max_offset));
            }
        }
    }

    #[test]
    fn busiest_class_picks_the_highest_count() {
        let report = fixture(
            "logical-tso",
            100.0,
            &[
                ("read-only", 40, 2.0),
                ("single-shard-insert", 900, 3.5),
                ("cross-shard-txn", 60, 8.0),
            ],
        );
        assert!(let Some(("single-shard-insert", _)) = busiest_class(&report));
        let (_, latency) = busiest_class(&report).expect("classes present");
        assert!(latency.count == 900);
        assert!((latency.p99_ms - 3.5).abs() < f64::EPSILON);

        let empty = fixture("logical-tso", 100.0, &[]);
        assert!(busiest_class(&empty) == None);
    }

    #[test]
    fn delta_headline_reports_percentage_or_na() {
        let left = fixture("logical-tso", 1000.0, &[]);
        let right = fixture("hlc(max_offset_ms=250)", 1250.0, &[]);
        assert!(
            delta_headline(&left, &right)
                == "mean tps: 1000.00 (logical-tso) vs 1250.00 (hlc(max_offset_ms=250)) — +25.00%"
        );

        let zero = fixture("logical-tso", 0.0, &[]);
        let headline = delta_headline(&zero, &right);
        assert!(headline.ends_with("n/a"), "headline {headline:?}");
    }
}
