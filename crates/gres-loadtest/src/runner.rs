//! Ties one scenario run together: cluster, workload, faults, sampling.
//!
//! Sequence: apply the mode override and re-validate → launch the cluster in
//! a scratch work dir → prepare the workload schema through the last node's
//! gateway (any gateway routes DDL and DML; picking a node that does not
//! host range 0 exercises exactly that) → start the `/proc` sampler → run
//! the workload (it warms up, then measures) concurrently with the fault
//! schedule anchored at the measurement-window start → stop the sampler →
//! shut the cluster down → assemble a [`RunReport`] and write
//! `<out_dir>/<scenario>-<mode-slug>.json` plus the rendered Markdown
//! alongside it.
//!
//! On a failure after launch the cluster is still shut down, the work dir
//! (holding every process log) is kept, and the error names its log
//! directory so the node logs can be inspected.

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context as _;
use crabka_gres_control::RegistryPolicy;
use crabka_units::prelude::*;
use tokio::time::Instant;

use crate::{
    cluster::{Binaries, Cluster, ClusterOptions, ProcessRoster, SqlEndpoint},
    config::LoadtestRuntimePolicy,
    external::{self, ExternalTarget},
    faults,
    metrics::ProcSampler,
    report::{
        self, AppliedFault, EfficiencySummary, ProcessResources, RunReport, ThroughputSummary,
        TopologySummary,
    },
    scenario::{ModeSpec, Scenario, ScenarioError},
    workload::{self, WorkloadOutcome},
};

/// Mode string recorded in external-mode reports; the crabka
/// timestamp-source modes do not apply to an external system.
pub const EXTERNAL_MODE: &str = "external";

/// Configuration for one scenario run.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// The parsed scenario.
    pub scenario: Scenario,
    /// Overrides the scenario's timestamp mode when set (the `compare`
    /// command runs the same scenario under both modes).
    pub mode_override: Option<ModeSpec>,
    /// Directory for reports and (on failure or request) retained logs.
    pub out_dir: PathBuf,
    /// Binaries to launch.
    pub binaries: Binaries,
    /// Keep the cluster work dir (data + logs) after a successful run.
    pub keep_work_dir: bool,
    /// Shared registry policy used by provisioning and spawned computes.
    pub registry_policy: RegistryPolicy,
    /// Harness-owned runtime policy.
    pub runtime_policy: LoadtestRuntimePolicy,
}

/// Paths of the two report files one run writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportPaths {
    /// Pretty-printed [`RunReport`] JSON.
    pub json: PathBuf,
    /// Rendered Markdown summary.
    pub markdown: PathBuf,
}

/// File-name slug for a mode: its display form with any parenthesized
/// suffix stripped (`logical-tso`, `hlc`).
#[must_use]
pub fn mode_slug(mode: ModeSpec) -> String {
    let display = mode.to_string();
    match display.split_once('(') {
        Some((slug, _)) => slug.to_owned(),
        None => display,
    }
}

/// Report file paths for one scenario × mode slug (a [`mode_slug`] or
/// [`EXTERNAL_MODE`]) under `out_dir`.
#[must_use]
pub fn report_paths(out_dir: &Path, scenario: &str, slug: &str) -> ReportPaths {
    ReportPaths {
        json: out_dir.join(format!("{scenario}-{slug}.json")),
        markdown: out_dir.join(format!("{scenario}-{slug}.md")),
    }
}

/// Runs one scenario end to end and writes its report files.
///
/// # Errors
///
/// Returns an error on harness failures (an invalid mode override, launch,
/// provisioning, IO); the error context names the retained log directory
/// when a cluster was involved. Workload errors caused by injected faults
/// are part of the report, not an error.
pub async fn run_scenario(config: RunConfig) -> anyhow::Result<RunReport> {
    let scenario = effective_scenario(config.scenario.clone(), config.mode_override)
        .context("apply mode override")?;
    let mode = scenario.mode;
    std::fs::create_dir_all(&config.out_dir)
        .with_context(|| format!("create out dir {}", config.out_dir.display()))?;
    let work_dir = prepare_work_dir(&config.out_dir, &scenario.name, mode, config.keep_work_dir)?;

    let options = cluster_options_for_run(&config, &scenario, mode, work_dir.path().to_path_buf());
    let mut cluster = match Cluster::launch(options).await {
        Ok(cluster) => cluster,
        Err(error) => {
            let kept = work_dir.persist();
            return Err(error.context(format!(
                "launch cluster for scenario {}; process logs kept under {}",
                scenario.name,
                kept.join("logs").display()
            )));
        }
    };
    // DDL and seeding go through the last node's gateway — in multi-node
    // topologies a node NOT hosting range 0 — proving any gateway routes
    // schema DDL (with its cluster-wide catalog barrier) and seed DML.
    let schema_node = cluster.node_count().saturating_sub(1);
    if let Err(error) = workload::prepare_schema_with_policy(
        &cluster.sql_endpoint(schema_node),
        &scenario.workload,
        &scenario.topology,
        config.runtime_policy,
    )
    .await
    {
        if let Err(shutdown_error) = cluster.shutdown().await {
            tracing::warn!(
                error = format!("{shutdown_error:#}"),
                "cluster shutdown failed"
            );
        }
        let kept = work_dir.persist();
        return Err(error.context(format!(
            "prepare workload schema for scenario {}; process logs kept under {}",
            scenario.name,
            kept.join("logs").display()
        )));
    }

    let driven = drive(&mut cluster, &scenario, config.runtime_policy).await;
    if let Err(error) = cluster.shutdown().await {
        // The report matters more than a clean teardown.
        tracing::warn!(error = format!("{error:#}"), "cluster shutdown failed");
    }
    let driven = match driven {
        Ok(driven) => driven,
        Err(error) => {
            let kept = work_dir.persist();
            return Err(error.context(format!(
                "scenario {} failed; node logs kept under {}",
                scenario.name,
                kept.join("logs").display()
            )));
        }
    };
    // Removes the work dir unless it is a kept directory under `out_dir`.
    drop(work_dir);

    let report = assemble_report(&scenario, &mode.to_string(), driven);
    write_reports(
        &report,
        &config.out_dir,
        &scenario.name,
        &mode_slug(mode),
        config.runtime_policy,
    )?;
    Ok(report)
}

fn cluster_options_for_run(
    config: &RunConfig,
    scenario: &Scenario,
    mode: ModeSpec,
    work_dir: PathBuf,
) -> ClusterOptions {
    ClusterOptions {
        topology: scenario.topology.clone(),
        mode,
        work_dir,
        binaries: config.binaries.clone(),
        registry_policy: config.registry_policy.clone(),
        runtime_policy: config.runtime_policy,
    }
}

/// Configuration for one external-cluster scenario run.
#[derive(Debug, Clone)]
pub struct ExternalRunConfig {
    /// The parsed scenario. Its `mode` is ignored (with a logged notice) and
    /// its fault timeline must be empty; `topology.ranges` still sets the
    /// number of workload tables.
    pub scenario: Scenario,
    /// The external system to drive.
    pub target: ExternalTarget,
    /// Directory for reports.
    pub out_dir: PathBuf,
    /// Harness-owned runtime policy.
    pub runtime_policy: LoadtestRuntimePolicy,
}

/// Runs one scenario against an external pgwire-speaking system and writes
/// the same report files as [`run_scenario`]: identical schema preparation,
/// workload, measurement window, and rendering — with no crabka processes
/// launched and no faults injected. The report's `mode` is
/// [`EXTERNAL_MODE`]. Resource rows cover the target's manual
/// `--external-pids` roster, or the local processes discovered listening on
/// the loopback endpoints' ports; when neither yields anything the run
/// proceeds with an empty roster (throughput and latency are unaffected).
///
/// # Errors
///
/// Returns an error if the scenario declares faults, an endpoint does not
/// resolve, schema preparation fails, or a report cannot be written.
pub async fn run_external_scenario(config: ExternalRunConfig) -> anyhow::Result<RunReport> {
    let ExternalRunConfig {
        scenario,
        target,
        out_dir,
        runtime_policy,
    } = config;
    external::validate_scenario(&scenario)?;
    tracing::info!(
        scenario_mode = %scenario.mode,
        "external mode: the scenario's timestamp-source mode is ignored \
         (the external system uses its own)"
    );
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("create out dir {}", out_dir.display()))?;
    let endpoints = target.sql_endpoints()?;
    let schema_endpoint = endpoints.last().context("no external endpoints")?;
    workload::prepare_schema_with_policy(
        schema_endpoint,
        &scenario.workload,
        &scenario.topology,
        runtime_policy,
    )
    .await
    .with_context(|| {
        format!(
            "prepare workload schema for scenario {} on {}",
            scenario.name, schema_endpoint.addr
        )
    })?;

    let processes = match &target.pids_override {
        Some(pids) => pids.clone(),
        None => external::pids_for_ports(&external::loopback_ports(&endpoints)),
    };
    if processes.is_empty() {
        tracing::warn!(
            "no local processes found for the external endpoints; resource usage will be \
             empty (throughput and latency are still measured; pass --external-pids to \
             attribute resources manually)"
        );
    } else {
        tracing::info!(?processes, "sampling external processes");
    }
    let roster = ProcessRoster::default();
    for process in processes {
        roster.push(process);
    }
    let sampler = ProcSampler::spawn(roster, runtime_policy.sample_interval);
    let started_unix_ms = window_start_unix_ms(SystemTime::now(), scenario.workload.warmup);
    let outcome = workload::run_with_policy(
        &endpoints,
        &scenario.workload,
        &scenario.topology,
        runtime_policy,
    )
    .await
    .context("drive workload")?;
    let resources = sampler.stop().await;
    let report = assemble_report(
        &scenario,
        EXTERNAL_MODE,
        Driven {
            started_unix_ms,
            outcome,
            resources,
            faults: Vec::new(),
        },
    );
    write_reports(
        &report,
        &out_dir,
        &scenario.name,
        EXTERNAL_MODE,
        runtime_policy,
    )?;
    Ok(report)
}

/// Serializes and writes the JSON + Markdown report pair for one run.
fn write_reports(
    report: &RunReport,
    out_dir: &Path,
    scenario: &str,
    slug: &str,
    policy: LoadtestRuntimePolicy,
) -> anyhow::Result<()> {
    let paths = report_paths(out_dir, scenario, slug);
    let json = serde_json::to_string_pretty(report).context("serialize report JSON")?;
    std::fs::write(&paths.json, json).with_context(|| format!("write {}", paths.json.display()))?;
    std::fs::write(
        &paths.markdown,
        report::render_markdown_with_policy(report, policy),
    )
    .with_context(|| format!("write {}", paths.markdown.display()))?;
    tracing::info!(
        json = %paths.json.display(),
        markdown = %paths.markdown.display(),
        "reports written"
    );
    Ok(())
}

/// The scenario actually run: the mode override applied, then re-validated
/// (an override can produce an invalid combination, e.g. `logical-tso` with
/// per-node clock skew configured).
fn effective_scenario(
    mut scenario: Scenario,
    mode_override: Option<ModeSpec>,
) -> Result<Scenario, ScenarioError> {
    if let Some(mode) = mode_override {
        scenario.mode = mode;
    }
    scenario.validate()?;
    Ok(scenario)
}

/// The cluster's scratch directory: a temp dir removed on success, or a
/// caller-requested kept directory under the out dir. Either variant is
/// persisted when a failure makes the process logs worth keeping.
#[derive(Debug)]
struct WorkDir {
    path: PathBuf,
    temp: Option<tempfile::TempDir>,
}

impl WorkDir {
    fn path(&self) -> &Path {
        &self.path
    }

    /// Keeps the directory on disk (disables temp-dir cleanup) and returns
    /// its path.
    fn persist(mut self) -> PathBuf {
        match self.temp.take() {
            Some(temp) => temp.keep(),
            None => self.path,
        }
    }
}

/// Creates the work dir: a fresh temp dir by default, or
/// `<out_dir>/<scenario>-<mode-slug>-work/` (recreated empty) when the
/// caller asked to keep it.
fn prepare_work_dir(
    out_dir: &Path,
    scenario: &str,
    mode: ModeSpec,
    keep: bool,
) -> anyhow::Result<WorkDir> {
    if keep {
        let path = out_dir.join(format!("{scenario}-{}-work", mode_slug(mode)));
        if path.exists() {
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("clear previous work dir {}", path.display()))?;
        }
        std::fs::create_dir_all(&path)
            .with_context(|| format!("create work dir {}", path.display()))?;
        Ok(WorkDir { path, temp: None })
    } else {
        let temp = tempfile::TempDir::new().context("create temp work dir")?;
        Ok(WorkDir {
            path: temp.path().to_path_buf(),
            temp: Some(temp),
        })
    }
}

/// Everything a successful measured window produced.
#[derive(Debug)]
struct Driven {
    started_unix_ms: u64,
    outcome: WorkloadOutcome,
    resources: Vec<ProcessResources>,
    faults: Vec<AppliedFault>,
}

/// Runs the measured window against a live cluster (schema already
/// prepared): `/proc` sampler + workload + fault schedule.
async fn drive(
    cluster: &mut Cluster,
    scenario: &Scenario,
    policy: LoadtestRuntimePolicy,
) -> anyhow::Result<Driven> {
    // The workload only needs the SQL endpoints, collected up front so the
    // workload future does not borrow the cluster (the fault schedule needs
    // it `&mut`).
    let endpoints: Vec<SqlEndpoint> = (0..cluster.node_count())
        .map(|node| cluster.sql_endpoint(node))
        .collect();
    // The live roster (not a one-shot process list) lets the sampler attach
    // nodes restarted by kill_node faults mid-window under `label#N` entries.
    let sampler = ProcSampler::spawn(cluster.process_roster(), policy.sample_interval);

    // The fault schedule anchors at the measurement-window start, one warmup
    // from now. This is an approximation: the workload starts its warmup a
    // moment after this line (it first probes for a live endpoint), so the
    // anchor runs slightly ahead of the true window. Keeping these
    // statements adjacent to the `join!` minimizes the skew.
    let window_start = Instant::now() + scenario.workload.warmup.to_std();
    let started_unix_ms = window_start_unix_ms(SystemTime::now(), scenario.workload.warmup);
    let (workload_result, faults_result) = tokio::join!(
        workload::run_with_policy(&endpoints, &scenario.workload, &scenario.topology, policy),
        faults::run_schedule_with_policy(
            &scenario.faults,
            scenario.topology.ranges,
            cluster,
            window_start,
            policy,
        ),
    );
    let resources = sampler.stop().await;
    let outcome = workload_result.context("drive workload")?;
    let faults = faults_result.context("apply fault schedule")?;
    Ok(Driven {
        started_unix_ms,
        outcome,
        resources,
        faults,
    })
}

/// Assembles the report from the measured window's outcome, resource
/// totals, and applied-fault log. `mode` is the report's mode string: the
/// scenario mode's display form, or [`EXTERNAL_MODE`].
fn assemble_report(scenario: &Scenario, mode: &str, driven: Driven) -> RunReport {
    let Driven {
        started_unix_ms,
        outcome,
        resources,
        faults,
    } = driven;
    let WorkloadOutcome {
        committed,
        failed,
        latency_by_class,
        timeline,
        errors,
        measured_wall,
    } = outcome;
    let mean_rate = rate_over(committed, measured_wall);
    let total_cpu = resources
        .iter()
        .fold(Time::ZERO, |total, process| total + process.cpu_time);
    let committed_txn_per_cpu = rate_over(committed, total_cpu);
    RunReport {
        scenario: scenario.name.clone(),
        description: scenario.description.clone(),
        mode: mode.to_owned(),
        started_unix_ms,
        topology: TopologySummary {
            nodes: scenario.topology.nodes,
            ranges: scenario.topology.ranges,
        },
        duration: measured_wall,
        throughput: ThroughputSummary {
            committed_txn: committed,
            failed_txn: failed,
            mean_rate,
        },
        latency_by_class: latency_by_class
            .into_iter()
            .map(|(class, summary)| (class.name().to_owned(), summary))
            .collect(),
        errors,
        timeline,
        resources,
        efficiency: EfficiencySummary {
            total_cpu,
            committed_txn_per_cpu,
        },
        faults,
    }
}

/// `count` events spread over `window`, or no rate at all when the window is
/// empty (dividing by it would be an infinity, and "nothing measured" is what
/// every reader of the report wants there).
fn rate_over(count: u64, window: Time) -> Frequency {
    if window <= Time::ZERO {
        return Frequency::ZERO;
    }
    u64_as_f64(count) / window
}

/// Unix milliseconds at which the measurement window opens: now, plus the
/// warmup the workload runs first.
fn window_start_unix_ms(now: SystemTime, warmup: Time) -> u64 {
    let warmup_ms = u64::try_from(warmup.millis_i64()).unwrap_or(0);
    unix_ms(now).saturating_add(warmup_ms)
}

/// Unix milliseconds of a wall-clock time (0 for a pre-epoch clock).
fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| u64::try_from(since.as_millis()).ok())
        .unwrap_or(0)
}

/// Lossless-for-practical-values `u64` → `f64` conversion built from exact
/// `u32` → `f64` conversions, avoiding a precision-losing `as` cast.
fn u64_as_f64(value: u64) -> f64 {
    const TWO_POW_32: f64 = 4_294_967_296.0;
    let high = u32::try_from(value >> 32).expect("u64 >> 32 fits in u32");
    let low = u32::try_from(value & 0xFFFF_FFFF).expect("masked to 32 bits");
    f64::from(high) * TWO_POW_32 + f64::from(low)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;
    use crate::{
        report::{ErrorSummary, LatencySummary, SecondSample},
        scenario::{MixSpec, RateSpec, TopologySpec, WorkloadSpec},
        workload::OpClass,
    };

    fn test_scenario(mode: ModeSpec, skew: &[(u16, Time)]) -> Scenario {
        Scenario {
            name: "steady".to_owned(),
            description: "Steady load.".to_owned(),
            topology: TopologySpec {
                nodes: 2,
                ranges: 3,
                clock_skew: skew.iter().copied().collect(),
                cpus_per_node: None,
                broker_cpus: None,
            },
            mode,
            workload: WorkloadSpec {
                connections: 8,
                rate: RateSpec::Saturate,
                warmup: secs(5),
                duration: secs(30),
                mix: MixSpec {
                    single_shard_insert: 1,
                    cross_shard_txn: 0,
                    read_only: 0,
                    contended_update: 0,
                },
                hot_rows: 100,
                zipf_exponent: 1.1,
            },
            faults: Vec::new(),
        }
    }

    #[test]
    fn mode_slug_strips_the_parenthesized_display_suffix() {
        let cases = [
            (ModeSpec::LogicalTso, "logical-tso"),
            (
                ModeSpec::Hlc {
                    max_offset: millis(250),
                },
                "hlc",
            ),
            (
                ModeSpec::Hlc {
                    max_offset: millis(1),
                },
                "hlc",
            ),
        ];
        for (mode, expected) in cases {
            assert!(mode_slug(mode) == expected, "mode {mode}");
        }
    }

    #[test]
    fn report_paths_join_scenario_and_slug() {
        let cases = [
            (
                mode_slug(ModeSpec::Hlc {
                    max_offset: millis(250),
                }),
                "out/tso-partition-hlc.json",
                "out/tso-partition-hlc.md",
            ),
            (
                EXTERNAL_MODE.to_owned(),
                "out/tso-partition-external.json",
                "out/tso-partition-external.md",
            ),
        ];
        for (slug, json, markdown) in cases {
            let paths = report_paths(Path::new("out"), "tso-partition", &slug);
            let expected = ReportPaths {
                json: PathBuf::from(json),
                markdown: PathBuf::from(markdown),
            };
            assert!(paths == expected, "slug {slug}");
        }
    }

    #[test]
    fn run_config_builds_cluster_options_with_the_same_registry_policy() {
        let policy = RegistryPolicy::new(
            3,
            crabka_units::millis(15_002),
            crabka_units::millis(252),
            crabka_units::millis(502),
            crabka_units::bytes(1_048_578),
        )
        .expect("policy");
        let config = RunConfig {
            runtime_policy: LoadtestRuntimePolicy::default(),
            scenario: test_scenario(ModeSpec::LogicalTso, &[]),
            mode_override: None,
            out_dir: PathBuf::from("/out"),
            binaries: Binaries {
                gres: PathBuf::from("/bin/gres"),
                broker: PathBuf::from("/bin/broker"),
                crabka_cli: PathBuf::from("/bin/crabka"),
            },
            keep_work_dir: false,
            registry_policy: policy.clone(),
        };
        let options = cluster_options_for_run(
            &config,
            &config.scenario,
            ModeSpec::LogicalTso,
            PathBuf::from("/work"),
        );
        assert!(options.registry_policy == policy);
        assert!(options.runtime_policy == config.runtime_policy);
    }

    #[test]
    fn mode_override_applies_and_revalidates() {
        // No override: the scenario's own mode stands.
        let scenario = effective_scenario(test_scenario(ModeSpec::LogicalTso, &[]), None)
            .expect("no override is valid");
        assert!(scenario.mode == ModeSpec::LogicalTso);

        // Override to HLC: applied and still valid.
        let scenario = effective_scenario(
            test_scenario(ModeSpec::LogicalTso, &[]),
            Some(ModeSpec::Hlc {
                max_offset: millis(300),
            }),
        )
        .expect("hlc override is valid");
        assert!(
            scenario.mode
                == ModeSpec::Hlc {
                    max_offset: millis(300)
                }
        );

        // Override to LogicalTso on a scenario with clock skew: the
        // re-validation must reject the combination.
        let result = effective_scenario(
            test_scenario(
                ModeSpec::Hlc {
                    max_offset: millis(250),
                },
                &[(0, millis(400))],
            ),
            Some(ModeSpec::LogicalTso),
        );
        assert!(let Err(ScenarioError::Invalid(message)) = result);
        assert!(message.contains("clock_skew requires hlc mode"));
    }

    #[test]
    fn external_validation_rejects_scenarios_with_faults() {
        use crate::scenario::{FaultAction, FaultEvent, FaultTarget, PartitionStyle};

        let mut scenario = test_scenario(ModeSpec::LogicalTso, &[]);
        external::validate_scenario(&scenario).expect("no faults is valid");

        scenario.faults.push(FaultEvent {
            at: secs(5),
            action: FaultAction::Partition {
                target: FaultTarget::Range(0),
                duration: secs(5),
                style: PartitionStyle::Blackhole,
            },
        });
        let error = external::validate_scenario(&scenario).expect_err("faults must be rejected");
        let message = format!("{error:#}");
        assert!(
            message.contains("external mode cannot inject faults"),
            "message {message:?}"
        );
    }

    #[test]
    fn assemble_report_computes_rates_and_keys_classes_by_name() {
        let scenario = test_scenario(ModeSpec::LogicalTso, &[]);
        let latency = LatencySummary {
            count: 1200,
            mean: micros(1500),
            p50: micros(1200),
            p95: millis(3),
            p99: micros(4500),
            p999: millis(6),
            max: millis(9),
        };
        let timeline = vec![SecondSample {
            t: Time::ZERO,
            committed: 1200,
            errors: 1,
            mean_latency: Some(micros(1500)),
        }];
        let errors = ErrorSummary {
            serialization_retries: 3,
            unavailable: 1,
            connection_errors: 0,
            other: 0,
        };
        let resources = vec![
            ProcessResources {
                label: "broker".to_owned(),
                pid: 100,
                cpu_time: secs(10),
                max_rss: kibibytes(1),
            },
            ProcessResources {
                label: "node0".to_owned(),
                pid: 200,
                cpu_time: secs(20),
                max_rss: kibibytes(2),
            },
        ];
        let faults = vec![AppliedFault {
            at: secs(20),
            description: "partition range:0 blackhole".to_owned(),
        }];
        let driven = Driven {
            started_unix_ms: 1_753_132_800_000,
            outcome: WorkloadOutcome {
                committed: 1200,
                failed: 3,
                latency_by_class: BTreeMap::from([(OpClass::SingleShardInsert, latency)]),
                timeline: timeline.clone(),
                errors,
                measured_wall: secs(60),
            },
            resources: resources.clone(),
            faults: faults.clone(),
        };
        let expected = RunReport {
            scenario: "steady".to_owned(),
            description: "Steady load.".to_owned(),
            mode: "logical-tso".to_owned(),
            started_unix_ms: 1_753_132_800_000,
            topology: TopologySummary {
                nodes: 2,
                ranges: 3,
            },
            duration: secs(60),
            throughput: ThroughputSummary {
                committed_txn: 1200,
                failed_txn: 3,
                mean_rate: per_sec(20),
            },
            latency_by_class: BTreeMap::from([("single-shard-insert".to_owned(), latency)]),
            errors,
            timeline,
            resources,
            efficiency: EfficiencySummary {
                total_cpu: secs(30),
                committed_txn_per_cpu: per_sec(40),
            },
            faults,
        };
        assert!(assemble_report(&scenario, "logical-tso", driven) == expected);
    }

    #[test]
    fn assemble_report_guards_zero_wall_time_and_zero_cpu() {
        let scenario = test_scenario(ModeSpec::LogicalTso, &[]);
        let driven = Driven {
            started_unix_ms: 0,
            outcome: WorkloadOutcome {
                committed: 500,
                failed: 0,
                latency_by_class: BTreeMap::new(),
                timeline: Vec::new(),
                errors: ErrorSummary::default(),
                measured_wall: Time::ZERO,
            },
            resources: Vec::new(),
            faults: Vec::new(),
        };
        let report = assemble_report(&scenario, EXTERNAL_MODE, driven);
        assert!(report.mode == EXTERNAL_MODE);
        assert!(
            report.throughput
                == ThroughputSummary {
                    committed_txn: 500,
                    failed_txn: 0,
                    mean_rate: Frequency::ZERO,
                }
        );
        assert!(
            report.efficiency
                == EfficiencySummary {
                    total_cpu: Time::ZERO,
                    committed_txn_per_cpu: Frequency::ZERO,
                }
        );
    }
}
