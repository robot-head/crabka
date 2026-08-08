//! CLI for the crabka-gres scalability and fault-injection harness.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::{Args, Parser, Subcommand};
use crabka_client_core::{ClientFrameMax, ConnectionDispatchQueueCapacity, FetchMinBytes};
use crabka_gres_control::{RegistryPolicy, RegistryReplicationFactor};
use crabka_gres_loadtest::{
    cluster::Binaries,
    config::{LoadtestRuntimePolicy, NonNegativeUsize, PositiveUsize},
    external::{self, ExternalTarget},
    report::{self, LatencySummary, RunReport},
    runner::{self, ExternalRunConfig, RunConfig},
    scenario::{ModeSpec, Scenario},
};
use crabka_units::{fmt::Human as _, prelude::*};
use tracing_subscriber::EnvFilter;

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
    /// Run one scenario and write its JSON and Markdown report.
    Run {
        #[command(flatten)]
        registry: RegistryOptions,
        #[command(flatten)]
        runtime: RuntimeOptions,
        /// Path to the scenario YAML.
        #[arg(long)]
        scenario: PathBuf,
        /// Override the scenario's timestamp-source mode, either
        /// `logical-tso` or `hlc`. It has no meaning with `--external`.
        #[arg(long, conflicts_with = "external")]
        mode: Option<String>,
        /// Output directory for reports.
        #[arg(long, default_value = "loadtest-out")]
        out: PathBuf,
        /// Keep the cluster work dir, with its data and logs, after a
        /// successful run.
        #[arg(long)]
        keep_work_dir: bool,
        #[command(flatten)]
        external: ExternalFlags,
    },
    /// Run one scenario under both timestamp modes and render a comparison.
    Compare {
        #[command(flatten)]
        registry: RegistryOptions,
        #[command(flatten)]
        runtime: RuntimeOptions,
        /// Path to the scenario YAML.
        #[arg(long)]
        scenario: PathBuf,
        /// Output directory for reports.
        #[arg(long, default_value = "loadtest-out")]
        out: PathBuf,
        /// Keep the cluster work dirs, with their data and logs, after
        /// successful runs.
        #[arg(long)]
        keep_work_dir: bool,
        /// Not supported here. `compare` contrasts crabka's timestamp-source
        /// modes on a harness-launched cluster. Use `run --external` for each
        /// target system instead.
        #[arg(long)]
        external: Option<String>,
    },
    /// Parse and validate a scenario without running it.
    Validate {
        /// Path to the scenario YAML.
        #[arg(long)]
        scenario: PathBuf,
    },
}

#[derive(Args)]
struct RuntimeOptions {
    #[arg(long, env = "CRABKA_GRES_LOADTEST_LAUNCH_TIMEOUT", default_value = "2m", value_parser = crabka_units::parse::positive_time)]
    launch_timeout: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_KILL_TIMEOUT", default_value = "10s", value_parser = crabka_units::parse::positive_time)]
    kill_timeout: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_LOG_DRAIN_TIMEOUT", default_value = "5s", value_parser = crabka_units::parse::positive_time)]
    log_drain_timeout: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_BROKER_POLL_INTERVAL", default_value = "100ms", value_parser = crabka_units::parse::positive_time)]
    broker_poll_interval: Time,
    #[arg(long = "topic-create-timeout", env = "CRABKA_GRES_LOADTEST_TOPIC_CREATE_TIMEOUT", default_value = "30s", value_parser = crabka_units::parse::positive_time)]
    loadtest_topic_create_timeout: Time,
    #[arg(
        long,
        env = "CRABKA_GRES_LOADTEST_LOG_TAIL_LINES",
        default_value = "40"
    )]
    log_tail_lines: PositiveUsize,
    #[arg(
        long,
        env = "CRABKA_GRES_LOADTEST_MAX_SERIALIZATION_RETRIES",
        default_value = "5"
    )]
    max_serialization_retries: NonNegativeUsize,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_OPERATION_TIMEOUT", default_value = "30s", value_parser = crabka_units::parse::positive_time)]
    operation_timeout: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_CONNECT_TIMEOUT", default_value = "5s", value_parser = crabka_units::parse::positive_time)]
    connect_timeout: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_STARTUP_DEADLINE", default_value = "30s", value_parser = crabka_units::parse::positive_time)]
    startup_deadline: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_STARTUP_RETRY_DELAY", default_value = "250ms", value_parser = crabka_units::parse::positive_time)]
    startup_retry_delay: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_SHUTDOWN_GRACE", default_value = "5s", value_parser = crabka_units::parse::positive_time)]
    shutdown_grace: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_RECONNECT_BACKOFF_MIN", default_value = "100ms", value_parser = crabka_units::parse::positive_time)]
    reconnect_backoff_min: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_RECONNECT_BACKOFF_MAX", default_value = "2s", value_parser = crabka_units::parse::positive_time)]
    reconnect_backoff_max: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_HISTOGRAM_MIN", default_value = "1us", value_parser = crabka_units::parse::positive_time)]
    histogram_min: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_HISTOGRAM_MAX", default_value = "60s", value_parser = crabka_units::parse::positive_time)]
    histogram_max: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_MIN_PACING_WAIT", default_value = "500us", value_parser = crabka_units::parse::positive_time)]
    min_pacing_wait: Time,
    #[arg(
        long,
        env = "CRABKA_GRES_LOADTEST_READ_SLICE_ROWS",
        default_value = "1024"
    )]
    read_slice_rows: PositiveUsize,
    #[arg(
        long,
        env = "CRABKA_GRES_LOADTEST_SEED_BATCH_ROWS",
        default_value = "500"
    )]
    seed_batch_rows: PositiveUsize,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_PROXY_MIN_BURST", default_value = "64KiB", value_parser = crabka_units::parse::positive_byte_size)]
    proxy_min_burst: ByteSize,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_PROXY_BURST_WINDOW", default_value = "100ms", value_parser = crabka_units::parse::positive_time)]
    proxy_burst_window: Time,
    #[arg(
        long,
        env = "CRABKA_GRES_LOADTEST_PROXY_DELAY_QUEUE_DEPTH",
        default_value = "256"
    )]
    proxy_delay_queue_depth: PositiveUsize,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_SAMPLE_INTERVAL", default_value = "1s", value_parser = crabka_units::parse::positive_time)]
    sample_interval: Time,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_FAULT_WINDOW", default_value = "5s", value_parser = crabka_units::parse::positive_time)]
    fault_window: Time,
    #[arg(
        long,
        env = "CRABKA_GRES_LOADTEST_TIMELINE_ROW_CAP",
        default_value = "60"
    )]
    timeline_row_cap: PositiveUsize,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_DEVIATION_THRESHOLD", default_value = "30%", value_parser = crabka_units::parse::unit_ratio)]
    deviation_threshold: Ratio,
    #[arg(long, env = "CRABKA_GRES_LOADTEST_MIN_FLAP_PERIOD", default_value = "1s", value_parser = crabka_units::parse::positive_time)]
    min_flap_period: Time,
    #[arg(long = "hlc-max-offset", env = "CRABKA_GRES_LOADTEST_HLC_MAX_OFFSET", default_value = "250ms", value_parser = crabka_units::parse::positive_time)]
    compare_hlc_max_offset: Time,
}

impl RuntimeOptions {
    fn policy(self) -> anyhow::Result<LoadtestRuntimePolicy> {
        let policy = LoadtestRuntimePolicy {
            launch_timeout: self.launch_timeout,
            kill_timeout: self.kill_timeout,
            log_drain_timeout: self.log_drain_timeout,
            broker_poll_interval: self.broker_poll_interval,
            topic_create_timeout: self.loadtest_topic_create_timeout,
            log_tail_lines: self.log_tail_lines,
            max_serialization_retries: self.max_serialization_retries,
            operation_timeout: self.operation_timeout,
            connect_timeout: self.connect_timeout,
            startup_deadline: self.startup_deadline,
            startup_retry_delay: self.startup_retry_delay,
            shutdown_grace: self.shutdown_grace,
            reconnect_backoff_min: self.reconnect_backoff_min,
            reconnect_backoff_max: self.reconnect_backoff_max,
            histogram_min: self.histogram_min,
            histogram_max: self.histogram_max,
            min_pacing_wait: self.min_pacing_wait,
            read_slice_rows: self.read_slice_rows,
            seed_batch_rows: self.seed_batch_rows,
            proxy_min_burst: self.proxy_min_burst,
            proxy_burst_window: self.proxy_burst_window,
            proxy_delay_queue_depth: self.proxy_delay_queue_depth,
            sample_interval: self.sample_interval,
            fault_window: self.fault_window,
            timeline_row_cap: self.timeline_row_cap,
            deviation_threshold: self.deviation_threshold,
            min_flap_period: self.min_flap_period,
            compare_hlc_max_offset: self.compare_hlc_max_offset,
        };
        policy.validate().map_err(anyhow::Error::msg)?;
        Ok(policy)
    }
}

#[derive(Args)]
struct RegistryOptions {
    #[arg(
        long = "client-dispatch-queue-capacity",
        env = "CRABKA_GRES_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = crabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long = "client-frame-max",
        env = "CRABKA_GRES_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
    #[arg(
        long = "registry-reader-fetch-min",
        env = "CRABKA_GRES_REGISTRY_READER_FETCH_MIN",
        default_value = "1B",
        value_parser = parse_fetch_min
    )]
    registry_reader_fetch_min: ByteSize,
    #[arg(
        long = "registry-replication-factor",
        env = "CRABKA_GRES_REGISTRY_REPLICATION_FACTOR",
        default_value = "1"
    )]
    replication_factor: RegistryReplicationFactor,
    #[arg(
        long = "registry-topic-create-timeout",
        env = "CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT",
        default_value = "15s",
        value_parser = crabka_units::parse::positive_time
    )]
    topic_create_timeout: Time,
    #[arg(
        long = "registry-reader-retry-backoff",
        env = "CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF",
        default_value = "250ms",
        value_parser = crabka_units::parse::positive_time
    )]
    reader_retry_backoff: Time,
    #[arg(
        long = "registry-fetch-max-wait",
        env = "CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT",
        default_value = "500ms",
        value_parser = crabka_units::parse::positive_time
    )]
    fetch_max_wait: Time,
    #[arg(
        long = "registry-fetch-partition-max",
        env = "CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX",
        default_value = "1MiB",
        value_parser = crabka_units::parse::positive_byte_size
    )]
    fetch_partition_max: ByteSize,
    #[arg(
        long = "registry-producer-dns-timeout",
        env = "CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT",
        value_parser = crabka_units::parse::positive_time
    )]
    producer_dns_timeout: Option<Time>,
    #[arg(
        long = "registry-reader-admin-dns-timeout",
        env = "CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT",
        value_parser = crabka_units::parse::positive_time
    )]
    reader_admin_dns_timeout: Option<Time>,
}

impl RegistryOptions {
    fn policy(&self) -> RegistryPolicy {
        let defaults = RegistryPolicy::default();

        RegistryPolicy::new(
            self.replication_factor.into_value(),
            self.topic_create_timeout,
            self.reader_retry_backoff,
            self.fetch_max_wait,
            self.fetch_partition_max,
        )
        .expect("validated registry options")
        .with_producer_dns_timeout(
            self.producer_dns_timeout
                .unwrap_or_else(|| defaults.producer_dns_timeout().time()),
        )
        .expect("validated registry producer DNS timeout")
        .with_reader_admin_dns_timeout(
            self.reader_admin_dns_timeout
                .unwrap_or_else(|| defaults.reader_admin_dns_timeout().time()),
        )
        .expect("validated registry reader/admin DNS timeout")
        .with_client_resource_policy(
            ConnectionDispatchQueueCapacity::new(self.client_dispatch_queue_capacity)
                .expect("validated client dispatch queue capacity"),
            ClientFrameMax::try_from(self.client_frame_max)
                .expect("validated client frame maximum"),
            FetchMinBytes::try_from(self.registry_reader_fetch_min)
                .expect("validated registry reader fetch minimum"),
        )
    }
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value =
        crabka_units::parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

fn parse_fetch_min(value: &str) -> Result<ByteSize, String> {
    let value =
        crabka_units::parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    FetchMinBytes::try_from(value).map(FetchMinBytes::size)
}

/// The `--external*` flag family. It benchmarks an existing pgwire-speaking
/// SQL system, such as `CockroachDB`, `YugabyteDB`, `PostgreSQL`, or a remote
/// crabka cluster, and launches no crabka cluster.
#[derive(Args)]
struct ExternalFlags {
    /// Comma-separated `host:port` SQL endpoints of the external system.
    /// This flag enables external mode. The harness then launches no crabka
    /// process, the scenario's faults must be empty, and the harness ignores
    /// the scenario's `mode`.
    #[arg(long)]
    external: Option<String>,
    /// SQL user for the external endpoints. It is required with
    /// `--external`.
    #[arg(long, requires = "external")]
    external_user: Option<String>,
    /// SQL password for the external endpoints. Omit it for no password.
    #[arg(long, requires = "external")]
    external_password: Option<String>,
    /// Database name on the external endpoints. It is required with
    /// `--external`.
    #[arg(long, requires = "external")]
    external_database: Option<String>,
    /// Manual resource roster, as comma-separated `label=pid` entries. It
    /// overrides the port-based discovery. Use it for a multi-process system,
    /// such as a `YugabyteDB` master and tserver, or when `/proc` discovery is
    /// not permitted.
    #[arg(long, requires = "external")]
    external_pids: Option<String>,
}

/// Builds the external target from the flag family. Without `--external` the
/// result is `None`. With it, the user and the database are required, because
/// external mode never guesses credentials, and the password defaults to
/// empty.
fn external_target(flags: ExternalFlags) -> anyhow::Result<Option<ExternalTarget>> {
    let Some(endpoints) = flags.external else {
        return Ok(None);
    };
    let endpoints = external::parse_endpoint_list(&endpoints)?;
    let user = flags
        .external_user
        .context("--external-user is required with --external")?;
    let database = flags
        .external_database
        .context("--external-database is required with --external")?;
    let pids_override = flags
        .external_pids
        .as_deref()
        .map(external::parse_pid_overrides)
        .transpose()?;
    Ok(Some(ExternalTarget {
        endpoints,
        user,
        password: flags.external_password.unwrap_or_default(),
        database,
        pids_override,
    }))
}

/// Rejects `compare --external`. The comparison is between crabka's
/// timestamp-source modes, and that needs a harness-launched cluster.
fn ensure_compare_is_internal(external: Option<&str>) -> anyhow::Result<()> {
    anyhow::ensure!(
        external.is_none(),
        "`compare` does not support --external: it contrasts crabka's timestamp-source \
         modes (logical-tso vs hlc) on a harness-launched cluster; benchmark an external \
         system with `run --external` once per target and diff the reports instead"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();
    match Cli::parse().command {
        CliCommand::Run {
            registry,
            runtime,
            scenario,
            mode,
            out,
            keep_work_dir,
            external,
        } => {
            run(
                &scenario,
                mode.as_deref(),
                &out,
                keep_work_dir,
                external,
                registry.policy(),
                runtime.policy()?,
            )
            .await
        }
        CliCommand::Compare {
            registry,
            runtime,
            scenario,
            out,
            keep_work_dir,
            external,
        } => {
            ensure_compare_is_internal(external.as_deref())?;
            compare(
                &scenario,
                &out,
                keep_work_dir,
                registry.policy(),
                runtime.policy()?,
            )
            .await
        }
        CliCommand::Validate { scenario } => validate(&scenario),
    }
}

/// The `run` subcommand. It drives one scenario, with an optional mode
/// override, and optionally against an external system.
async fn run(
    scenario_path: &Path,
    mode: Option<&str>,
    out: &Path,
    keep_work_dir: bool,
    external: ExternalFlags,
    registry_policy: RegistryPolicy,
    runtime_policy: LoadtestRuntimePolicy,
) -> anyhow::Result<()> {
    let scenario = load_scenario(scenario_path)?;
    if let Some(target) = external_target(external)? {
        let report = runner::run_external_scenario(ExternalRunConfig {
            scenario,
            target,
            out_dir: out.to_path_buf(),
            runtime_policy,
        })
        .await?;
        print_summary(&report, out, runner::EXTERNAL_MODE);
        return Ok(());
    }
    let mode_override = parse_mode(mode, runtime_policy.compare_hlc_max_offset)?;
    let effective_mode = mode_override.unwrap_or(scenario.mode);
    let binaries = Binaries::resolve()?;
    let report = runner::run_scenario(RunConfig {
        scenario,
        mode_override,
        out_dir: out.to_path_buf(),
        binaries,
        keep_work_dir,
        registry_policy,
        runtime_policy,
    })
    .await?;
    print_summary(&report, out, &runner::mode_slug(effective_mode));
    Ok(())
}

/// The `compare` subcommand. It runs the same scenario under `logical-tso`
/// and then `hlc`, and adds a side-by-side comparison report.
async fn compare(
    scenario_path: &Path,
    out: &Path,
    keep_work_dir: bool,
    registry_policy: RegistryPolicy,
    runtime_policy: LoadtestRuntimePolicy,
) -> anyhow::Result<()> {
    let scenario = load_scenario(scenario_path)?;
    let binaries = Binaries::resolve()?;
    let hlc = match scenario.mode {
        hlc @ ModeSpec::Hlc { .. } => hlc,
        ModeSpec::LogicalTso => ModeSpec::Hlc {
            max_offset: runtime_policy.compare_hlc_max_offset,
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
            registry_policy: registry_policy.clone(),
            runtime_policy,
        })
        .await
        .with_context(|| format!("run scenario {} under {mode}", scenario.name))?;
        print_summary(&report, out, &runner::mode_slug(mode));
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

/// The `validate` subcommand. It parses and validates the scenario, then
/// reports `ok` or fails.
fn validate(scenario_path: &Path) -> anyhow::Result<()> {
    let scenario = load_scenario(scenario_path)?;
    println!("ok: {}", scenario.name);
    Ok(())
}

/// Loads and validates a scenario. The error names the path.
fn load_scenario(path: &Path) -> anyhow::Result<Scenario> {
    Scenario::from_yaml_file(path).with_context(|| format!("load scenario {}", path.display()))
}

/// Maps the `--mode` string to a mode override.
fn parse_mode(mode: Option<&str>, hlc_max_offset: Time) -> anyhow::Result<Option<ModeSpec>> {
    match mode {
        None => Ok(None),
        Some("logical-tso") => Ok(Some(ModeSpec::LogicalTso)),
        Some("hlc") => Ok(Some(ModeSpec::Hlc {
            max_offset: hlc_max_offset,
        })),
        Some(other) => anyhow::bail!("unknown mode {other:?}: expected `logical-tso` or `hlc`"),
    }
}

/// Prints the short human summary of one run to stdout. `slug` names the
/// report files. It is a mode slug, or [`runner::EXTERNAL_MODE`].
fn print_summary(report: &RunReport, out_dir: &Path, slug: &str) {
    let paths = runner::report_paths(out_dir, &report.scenario, slug);
    println!("scenario:  {} ({})", report.scenario, report.mode);
    println!(
        "committed: {} txn, {:.2} tps mean, {} failed",
        report.throughput.committed_txn,
        report.throughput.mean_rate.per_sec_f64(),
        report.throughput.failed_txn
    );
    match busiest_class(report) {
        Some((class, latency)) => println!(
            "p99:       {} ({class}, {} ops)",
            latency.p99.human(),
            latency.count
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
    let left_rate = left.throughput.mean_rate;
    let right_rate = right.throughput.mean_rate;
    let delta = if left_rate > Frequency::ZERO {
        let relative: Ratio = (right_rate - left_rate) / left_rate;
        format!("{:+.2}%", relative.percent_f64())
    } else {
        "n/a".to_owned()
    };
    format!(
        "mean tps: {:.2} ({}) vs {:.2} ({}) — {delta}",
        left_rate.per_sec_f64(),
        left.mode,
        right_rate.per_sec_f64(),
        right.mode
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

    #[test]
    fn registry_policy_options_use_exact_defaults_and_validation() {
        let defaults =
            Cli::try_parse_from(["loadtest", "run", "--scenario=test.yaml"]).expect("defaults");
        let CliCommand::Run {
            registry, runtime, ..
        } = defaults.command
        else {
            panic!("run");
        };
        assert!(registry.policy() == crabka_gres_control::RegistryPolicy::default());
        assert!(runtime.policy().expect("runtime defaults") == LoadtestRuntimePolicy::default());
        for option in [
            "--registry-replication-factor=0",
            "--registry-replication-factor=32768",
            "--registry-topic-create-timeout=0ms",
            "--registry-reader-retry-backoff=0ms",
            "--registry-fetch-max-wait=0ms",
            "--registry-fetch-partition-max=0B",
            "--registry-producer-dns-timeout=0ms",
            "--registry-reader-admin-dns-timeout=0ms",
            "--client-dispatch-queue-capacity=0",
            "--client-frame-max=101MiB",
            "--registry-reader-fetch-min=0B",
        ] {
            assert!(
                Cli::try_parse_from(["loadtest", "run", "--scenario=test.yaml", option]).is_err()
            );
        }
        let invalid = Cli::try_parse_from([
            "loadtest",
            "run",
            "--scenario=test.yaml",
            "--reconnect-backoff-min=3s",
            "--reconnect-backoff-max=2s",
        ])
        .expect("scalar values parse");
        let CliCommand::Run { runtime, .. } = invalid.command else {
            panic!("run");
        };
        assert!(runtime.policy().is_err());
    }

    #[test]
    fn registry_policy_options_read_environment_and_prefer_cli() {
        const CHILD: &str = "CRABKA_TEST_LOADTEST_REGISTRY_ENV_CHILD";
        let vars = [
            ("CRABKA_GRES_REGISTRY_REPLICATION_FACTOR", "2"),
            ("CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT", "15001ms"),
            ("CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF", "251ms"),
            ("CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT", "501ms"),
            ("CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX", "1048577B"),
            ("CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT", "37ms"),
            ("CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT", "37ms"),
            ("CRABKA_GRES_CLIENT_DISPATCH_QUEUE_CAPACITY", "7"),
            ("CRABKA_GRES_CLIENT_FRAME_MAX", "32KiB"),
            ("CRABKA_GRES_REGISTRY_READER_FETCH_MIN", "3B"),
        ];
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
                .args([
                    "--exact",
                    "tests::registry_policy_options_read_environment_and_prefer_cli",
                ])
                .env(CHILD, "1")
                .envs(vars)
                .status()
                .expect("child test");
            assert!(status.success());
            return;
        }
        let environment =
            Cli::try_parse_from(["loadtest", "run", "--scenario=test.yaml"]).expect("environment");
        let CliCommand::Run { registry, .. } = environment.command else {
            panic!("run");
        };
        let environment_policy = RegistryPolicy::new(
            2,
            crabka_units::millis(15_001),
            crabka_units::millis(251),
            crabka_units::millis(501),
            bytes(1_048_577),
        )
        .expect("policy")
        .with_producer_dns_timeout(crabka_units::millis(37))
        .expect("environment DNS timeout")
        .with_reader_admin_dns_timeout(crabka_units::millis(37))
        .expect("environment reader/admin DNS timeout")
        .with_client_resource_policy(
            ConnectionDispatchQueueCapacity::new(7).unwrap(),
            ClientFrameMax::try_from(kibibytes(32)).unwrap(),
            FetchMinBytes::try_from(bytes(3)).unwrap(),
        );
        assert!(registry.policy() == environment_policy);
        let cli = Cli::try_parse_from([
            "loadtest",
            "run",
            "--scenario=test.yaml",
            "--registry-replication-factor=3",
            "--registry-topic-create-timeout=15002ms",
            "--registry-reader-retry-backoff=252ms",
            "--registry-fetch-max-wait=502ms",
            "--registry-fetch-partition-max=1048578B",
            "--registry-producer-dns-timeout=47ms",
            "--registry-reader-admin-dns-timeout=47ms",
            "--client-dispatch-queue-capacity=9",
            "--client-frame-max=64KiB",
            "--registry-reader-fetch-min=5B",
        ])
        .expect("CLI over environment");
        let CliCommand::Run { registry, .. } = cli.command else {
            panic!("run");
        };
        let cli_policy = RegistryPolicy::new(
            3,
            crabka_units::millis(15_002),
            crabka_units::millis(252),
            crabka_units::millis(502),
            bytes(1_048_578),
        )
        .expect("policy")
        .with_producer_dns_timeout(crabka_units::millis(47))
        .expect("CLI DNS timeout")
        .with_reader_admin_dns_timeout(crabka_units::millis(47))
        .expect("CLI reader/admin DNS timeout")
        .with_client_resource_policy(
            ConnectionDispatchQueueCapacity::new(9).unwrap(),
            ClientFrameMax::try_from(kibibytes(64)).unwrap(),
            FetchMinBytes::try_from(bytes(5)).unwrap(),
        );
        assert!(registry.policy() == cli_policy);
    }

    fn fixture(mode: &str, mean_rate: Frequency, classes: &[(&str, u64, Time)]) -> RunReport {
        RunReport {
            scenario: "steady".to_owned(),
            description: String::new(),
            mode: mode.to_owned(),
            started_unix_ms: 0,
            topology: TopologySummary {
                nodes: 2,
                ranges: 3,
            },
            duration: secs(60),
            throughput: ThroughputSummary {
                committed_txn: 100,
                failed_txn: 0,
                mean_rate,
            },
            latency_by_class: classes
                .iter()
                .map(|(class, count, p99)| {
                    (
                        (*class).to_owned(),
                        LatencySummary {
                            count: *count,
                            mean: millis(1),
                            p50: millis(1),
                            p95: millis(2),
                            p99: *p99,
                            p999: millis(5),
                            max: millis(9),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            errors: ErrorSummary::default(),
            timeline: Vec::new(),
            resources: Vec::new(),
            efficiency: EfficiencySummary {
                total_cpu: secs(1),
                committed_txn_per_cpu: per_sec(100),
            },
            faults: Vec::new(),
        }
    }

    #[test]
    fn parse_mode_maps_strings_to_mode_specs() {
        let cases = [
            (None, millis(250), Some(None)),
            (
                Some("logical-tso"),
                millis(250),
                Some(Some(ModeSpec::LogicalTso)),
            ),
            (
                Some("hlc"),
                millis(300),
                Some(Some(ModeSpec::Hlc {
                    max_offset: millis(300),
                })),
            ),
            (Some("banana"), millis(250), None),
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
            per_sec(100),
            &[
                ("read-only", 40, millis(2)),
                ("single-shard-insert", 900, micros(3500)),
                ("cross-shard-txn", 60, millis(8)),
            ],
        );
        assert!(let Some(("single-shard-insert", _)) = busiest_class(&report));
        let (_, latency) = busiest_class(&report).expect("classes present");
        assert!(latency.count == 900);
        assert!(latency.p99 == micros(3500));

        let empty = fixture("logical-tso", per_sec(100), &[]);
        assert!(busiest_class(&empty) == None);
    }

    #[test]
    fn external_target_requires_user_and_database_and_defaults_password() {
        use crabka_gres_loadtest::{cluster::ProcessInfo, external::HostPort};

        /// One flag-combination case. It holds
        /// `(external, user, password, database, pids)` and whether the build
        /// of the target must succeed.
        struct Case {
            flags: ExternalFlags,
            expected: Result<Option<ExternalTarget>, &'static str>,
        }
        let flags = |external: Option<&str>,
                     user: Option<&str>,
                     password: Option<&str>,
                     database: Option<&str>,
                     pids: Option<&str>| ExternalFlags {
            external: external.map(str::to_owned),
            external_user: user.map(str::to_owned),
            external_password: password.map(str::to_owned),
            external_database: database.map(str::to_owned),
            external_pids: pids.map(str::to_owned),
        };
        let cases = [
            // No --external: external mode is off regardless of the rest.
            Case {
                flags: flags(None, None, None, None, None),
                expected: Ok(None),
            },
            // Fully specified, with a manual pid roster.
            Case {
                flags: flags(
                    Some("db1:5432, db2:5433"),
                    Some("roach"),
                    Some("s3cret"),
                    Some("bench"),
                    Some("master=1,tserver=2"),
                ),
                expected: Ok(Some(ExternalTarget {
                    endpoints: vec![
                        HostPort {
                            host: "db1".to_owned(),
                            port: 5432,
                        },
                        HostPort {
                            host: "db2".to_owned(),
                            port: 5433,
                        },
                    ],
                    user: "roach".to_owned(),
                    password: "s3cret".to_owned(),
                    database: "bench".to_owned(),
                    pids_override: Some(vec![
                        ProcessInfo {
                            label: "master".to_owned(),
                            pid: 1,
                        },
                        ProcessInfo {
                            label: "tserver".to_owned(),
                            pid: 2,
                        },
                    ]),
                })),
            },
            // Password omitted: empty (no password), pids omitted: discover.
            Case {
                flags: flags(
                    Some("localhost:5432"),
                    Some("postgres"),
                    None,
                    Some("postgres"),
                    None,
                ),
                expected: Ok(Some(ExternalTarget {
                    endpoints: vec![HostPort {
                        host: "localhost".to_owned(),
                        port: 5432,
                    }],
                    user: "postgres".to_owned(),
                    password: String::new(),
                    database: "postgres".to_owned(),
                    pids_override: None,
                })),
            },
            Case {
                flags: flags(Some("localhost:5432"), None, None, Some("postgres"), None),
                expected: Err("--external-user is required"),
            },
            Case {
                flags: flags(Some("localhost:5432"), Some("postgres"), None, None, None),
                expected: Err("--external-database is required"),
            },
            Case {
                flags: flags(Some("not-an-endpoint"), Some("u"), None, Some("d"), None),
                expected: Err("invalid endpoint"),
            },
            Case {
                flags: flags(
                    Some("localhost:5432"),
                    Some("u"),
                    None,
                    Some("d"),
                    Some("bad"),
                ),
                expected: Err("expected label=pid"),
            },
        ];
        for (index, case) in cases.into_iter().enumerate() {
            match (external_target(case.flags), case.expected) {
                (Ok(target), Ok(expected)) => assert!(target == expected, "case {index}"),
                (Err(error), Err(fragment)) => {
                    let message = format!("{error:#}");
                    assert!(
                        message.contains(fragment),
                        "case {index}: expected {fragment:?} in {message:?}"
                    );
                }
                (result, expected) => panic!(
                    "case {index}: got {result:?}, expected success = {}",
                    expected.is_ok()
                ),
            }
        }
    }

    #[test]
    fn compare_rejects_external_with_a_clear_error() {
        assert!(let Ok(()) = ensure_compare_is_internal(None));
        let error = ensure_compare_is_internal(Some("db:5432")).expect_err("must reject");
        let message = format!("{error:#}");
        assert!(
            message.contains("`compare` does not support --external"),
            "message {message:?}"
        );
        assert!(message.contains("run --external"), "message {message:?}");
    }

    #[test]
    fn cli_couples_external_flags_to_external_and_conflicts_with_mode() {
        let base = ["crabka-gres-loadtest", "run", "--scenario", "s.yaml"];
        // (extra args, parse must succeed)
        let cases: [(&[&str], bool); 6] = [
            (&[], true),
            (
                &[
                    "--external",
                    "localhost:5432",
                    "--external-user",
                    "u",
                    "--external-database",
                    "d",
                ],
                true,
            ),
            // --mode is meaningless against an external system.
            (&["--external", "localhost:5432", "--mode", "hlc"], false),
            // The sub-flags require --external itself.
            (&["--external-user", "u"], false),
            (&["--external-database", "d"], false),
            (&["--external-pids", "db=1"], false),
        ];
        for (extra, ok) in cases {
            let args = base.iter().chain(extra).copied();
            let result = Cli::try_parse_from(args);
            assert!(result.is_ok() == ok, "extra args {extra:?}");
        }
    }

    #[test]
    fn delta_headline_reports_percentage_or_na() {
        let left = fixture("logical-tso", per_sec(1000), &[]);
        let right = fixture("hlc(max_offset=250ms)", per_sec(1250), &[]);
        assert!(
            delta_headline(&left, &right)
                == "mean tps: 1000.00 (logical-tso) vs 1250.00 (hlc(max_offset=250ms)) — +25.00%"
        );

        let zero = fixture("logical-tso", Frequency::ZERO, &[]);
        let headline = delta_headline(&zero, &right);
        assert!(headline.ends_with("n/a"), "headline {headline:?}");
    }
}
