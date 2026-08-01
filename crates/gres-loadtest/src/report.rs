//! Serialisable run results and their Markdown rendering.
//!
//! [`RunReport`] is the harness's durable output: written as JSON next to a
//! rendered Markdown summary. [`render_comparison`] lines up two reports of
//! the same scenario under different timestamp modes.
//!
//! Dimensioned fields are [`crabka_units`] quantities. Values a person reads
//! off the report — the applied-fault offsets, peak RSS, the headline rates —
//! serialize in their human form (`"20s"`, `"512MiB"`, `"1000/s"`); values
//! that get compared or plotted — latency percentiles, the per-second
//! timeline, CPU totals — serialize as exact integers in a named unit, so a
//! plotting script never has to parse a suffix.

use std::{
    collections::BTreeMap,
    fmt::{self, Write as _},
};

use crabka_units::{
    fmt::Human as _,
    prelude::*,
    serde_units::{
        human::{byte_size, frequency, time},
        numeric::{millis_i64, nanos_i64, option_nanos_i64, secs_i64},
    },
};
use serde::{Deserialize, Serialize};

use crate::config::LoadtestRuntimePolicy;

/// Complete result of one scenario run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunReport {
    /// Scenario name.
    pub scenario: String,
    /// Scenario description.
    pub description: String,
    /// Timestamp-source mode the run used (display form of `ModeSpec`).
    pub mode: String,
    /// Wall-clock start of the measurement window (unix milliseconds).
    pub started_unix_ms: u64,
    /// Topology summary.
    pub topology: TopologySummary,
    /// Measured window length (excludes warmup).
    #[serde(with = "millis_i64")]
    pub duration: Time,
    /// Aggregate throughput over the measurement window.
    pub throughput: ThroughputSummary,
    /// Latency percentiles per operation class (kebab-case class names).
    pub latency_by_class: BTreeMap<String, LatencySummary>,
    /// Error and retry counts over the measurement window.
    pub errors: ErrorSummary,
    /// Per-second progress timeline (throughput dips make faults visible).
    pub timeline: Vec<SecondSample>,
    /// Per-process resource usage over the measurement window.
    pub resources: Vec<ProcessResources>,
    /// Cluster-wide efficiency derived from throughput and resources.
    pub efficiency: EfficiencySummary,
    /// Faults actually applied, in application order.
    pub faults: Vec<AppliedFault>,
}

/// Cluster shape a run used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologySummary {
    /// Compute node count.
    pub nodes: u16,
    /// Range count.
    pub ranges: u16,
}

/// Aggregate throughput over the measurement window.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ThroughputSummary {
    /// Transactions committed.
    pub committed_txn: u64,
    /// Transactions that ultimately failed (after retries).
    pub failed_txn: u64,
    /// Mean committed transaction rate.
    #[serde(with = "frequency")]
    pub mean_rate: Frequency,
}

/// Latency distribution for one operation class.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    /// Operations measured.
    pub count: u64,
    /// Mean latency.
    #[serde(with = "nanos_i64")]
    pub mean: Time,
    /// 50th percentile.
    #[serde(with = "nanos_i64")]
    pub p50: Time,
    /// 95th percentile.
    #[serde(with = "nanos_i64")]
    pub p95: Time,
    /// 99th percentile.
    #[serde(with = "nanos_i64")]
    pub p99: Time,
    /// 99.9th percentile.
    #[serde(with = "nanos_i64")]
    pub p999: Time,
    /// Maximum observed.
    #[serde(with = "nanos_i64")]
    pub max: Time,
}

/// Error and retry counts over the measurement window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ErrorSummary {
    /// Serialization failures that were retried (the retry succeeded or was
    /// itself counted elsewhere).
    pub serialization_retries: u64,
    /// Operations failed because the cluster was unreachable or unavailable.
    pub unavailable: u64,
    /// Connections dropped or refused mid-run.
    pub connection_errors: u64,
    /// Any other errors.
    pub other: u64,
}

/// One second of workload progress.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SecondSample {
    /// Offset from the start of the measurement window.
    #[serde(with = "secs_i64")]
    pub t: Time,
    /// Transactions committed during this second.
    pub committed: u64,
    /// Errors observed during this second.
    pub errors: u64,
    /// Mean latency of operations completed this second, if any.
    #[serde(with = "option_nanos_i64")]
    pub mean_latency: Option<Time>,
}

/// Resource usage of one launched process over the measurement window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessResources {
    /// Process label (`broker`, `node0`, ...).
    pub label: String,
    /// OS process id.
    pub pid: u32,
    /// CPU consumed, as core-time (user + system).
    #[serde(with = "millis_i64")]
    pub cpu_time: Time,
    /// Peak resident set size observed.
    #[serde(with = "byte_size")]
    pub max_rss: ByteSize,
}

/// Cluster-wide efficiency derived from throughput and resources.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EfficiencySummary {
    /// Total CPU consumed by broker + nodes, as core-time.
    #[serde(with = "millis_i64")]
    pub total_cpu: Time,
    /// Committed transactions per consumed CPU core-second.
    #[serde(with = "frequency")]
    pub committed_txn_per_cpu: Frequency,
}

/// A fault the scheduler actually applied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedFault {
    /// Offset from the start of the measurement window.
    #[serde(with = "time")]
    pub at: Time,
    /// Human-readable description (e.g. `partition range:0 blackhole 10s`).
    pub description: String,
}

/// Renders one run as a Markdown summary: headline throughput/efficiency,
/// per-class latency table, error table, per-process resource table, fault
/// log, and a compact per-second table of the interesting seconds (those
/// near an applied fault or whose committed count deviates more than
/// the configured threshold from the run's median).
#[must_use]
pub fn render_markdown(report: &RunReport) -> String {
    render_markdown_with_policy(report, LoadtestRuntimePolicy::default())
}

/// Renders one run using explicit report-selection policy.
#[must_use]
pub fn render_markdown_with_policy(report: &RunReport, policy: LoadtestRuntimePolicy) -> String {
    let mut out = String::new();
    push_header(&mut out, report);
    push_headline(&mut out, report);
    push_latency(&mut out, report);
    push_errors(&mut out, report);
    push_resources(&mut out, report);
    push_faults(&mut out, report);
    push_timeline(&mut out, report, policy);
    out
}

/// Renders two runs of the same scenario (typically `logical-tso` vs `hlc`)
/// side by side with absolute values and relative deltas (`(right − left) /
/// left`, `n/a` when the left value is zero), both runs' fault lists, and a
/// notes line calling out the largest absolute delta.
#[must_use]
pub fn render_comparison(left: &RunReport, right: &RunReport) -> String {
    let mut out = String::new();
    push_fmt(
        &mut out,
        format_args!("# {} — {} vs {}\n\n", left.scenario, left.mode, right.mode),
    );
    push_fmt(
        &mut out,
        format_args!("| Metric | {} | {} | Delta |\n", left.mode, right.mode),
    );
    out.push_str("| :-- | ---: | ---: | ---: |\n");
    let rows = compare_rows(left, right);
    for row in &rows {
        push_fmt(
            &mut out,
            format_args!(
                "| {} | {} | {} | {} |\n",
                row.metric,
                row.left,
                row.right,
                fmt_delta(row.delta)
            ),
        );
    }
    out.push('\n');
    let notes = rows
        .iter()
        .filter_map(|row| row.delta.map(|delta| (delta.abs().as_f64(), row)))
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map_or_else(
            || "Notes: no comparable deltas.".to_string(),
            |(_, row)| {
                format!(
                    "Notes: largest delta — {} ({}).",
                    row.metric,
                    fmt_delta(row.delta)
                )
            },
        );
    out.push_str(&notes);
    out.push_str("\n\n");
    push_fmt(&mut out, format_args!("## Faults — {}\n\n", left.mode));
    push_fault_list(&mut out, &left.faults);
    out.push('\n');
    push_fmt(&mut out, format_args!("## Faults — {}\n\n", right.mode));
    push_fault_list(&mut out, &right.faults);
    out
}

fn push_header(out: &mut String, report: &RunReport) {
    push_fmt(
        out,
        format_args!("# {} — {}\n\n", report.scenario, report.mode),
    );
    push_fmt(out, format_args!("{}\n\n", report.description));
    push_fmt(
        out,
        format_args!(
            "Started {} unix ms — duration {} s — topology {} nodes / {} ranges.\n\n",
            report.started_unix_ms,
            fmt2(report.duration.secs_f64()),
            report.topology.nodes,
            report.topology.ranges,
        ),
    );
}

fn push_headline(out: &mut String, report: &RunReport) {
    out.push_str("## Headline\n\n");
    out.push_str(
        "| Committed txn | Failed txn | Mean TPS | Total CPU core-s | Committed txn / CPU-s | Peak RSS |\n",
    );
    out.push_str("| ---: | ---: | ---: | ---: | ---: | ---: |\n");
    push_fmt(
        out,
        format_args!(
            "| {} | {} | {} | {} | {} | {} |\n\n",
            report.throughput.committed_txn,
            report.throughput.failed_txn,
            fmt2(report.throughput.mean_rate.per_sec_f64()),
            fmt2(report.efficiency.total_cpu.secs_f64()),
            fmt2(report.efficiency.committed_txn_per_cpu.per_sec_f64()),
            peak_rss(report).human(),
        ),
    );
}

fn push_latency(out: &mut String, report: &RunReport) {
    out.push_str("## Latency by class\n\n");
    out.push_str("| Class | Count | Mean | p50 | p95 | p99 | p99.9 | Max |\n");
    out.push_str("| :-- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for (class, latency) in &report.latency_by_class {
        push_fmt(
            out,
            format_args!(
                "| {class} | {} | {} | {} | {} | {} | {} | {} |\n",
                latency.count,
                latency.mean.human(),
                latency.p50.human(),
                latency.p95.human(),
                latency.p99.human(),
                latency.p999.human(),
                latency.max.human(),
            ),
        );
    }
    out.push('\n');
}

fn push_errors(out: &mut String, report: &RunReport) {
    out.push_str("## Errors\n\n");
    out.push_str("| Serialization retries | Unavailable | Connection errors | Other |\n");
    out.push_str("| ---: | ---: | ---: | ---: |\n");
    push_fmt(
        out,
        format_args!(
            "| {} | {} | {} | {} |\n\n",
            report.errors.serialization_retries,
            report.errors.unavailable,
            report.errors.connection_errors,
            report.errors.other,
        ),
    );
}

fn push_resources(out: &mut String, report: &RunReport) {
    out.push_str("## Resources\n\n");
    out.push_str("| Process | PID | CPU core-s | Max RSS |\n");
    out.push_str("| :-- | ---: | ---: | ---: |\n");
    for resources in &report.resources {
        push_fmt(
            out,
            format_args!(
                "| {} | {} | {} | {} |\n",
                resources.label,
                resources.pid,
                fmt2(resources.cpu_time.secs_f64()),
                resources.max_rss.human(),
            ),
        );
    }
    out.push('\n');
}

fn push_faults(out: &mut String, report: &RunReport) {
    out.push_str("## Faults\n\n");
    push_fault_list(out, &report.faults);
    out.push('\n');
}

fn push_fault_list(out: &mut String, faults: &[AppliedFault]) {
    if faults.is_empty() {
        out.push_str("none\n");
        return;
    }
    for fault in faults {
        push_fmt(
            out,
            format_args!("- t={} — {}\n", fault.at.human(), fault.description),
        );
    }
}

fn push_timeline(out: &mut String, report: &RunReport, policy: LoadtestRuntimePolicy) {
    if report.timeline.is_empty() {
        return;
    }
    out.push_str("## Timeline (interesting seconds)\n\n");
    let interesting = interesting_seconds_with_policy(report, policy);
    if interesting.is_empty() {
        push_fmt(
            out,
            format_args!(
                "No interesting seconds — committed stayed within {} of the median across {} \
             samples and no faults applied.\n\n",
                policy.deviation_threshold.human(),
                report.timeline.len()
            ),
        );
        return;
    }
    out.push_str("| t | Committed | Errors | Mean latency |\n");
    out.push_str("| ---: | ---: | ---: | ---: |\n");
    for sample in interesting.iter().take(policy.timeline_row_cap.get()) {
        push_fmt(
            out,
            format_args!(
                "| {} | {} | {} | {} |\n",
                sample.t.human(),
                sample.committed,
                sample.errors,
                fmt_mean_latency(sample.mean_latency),
            ),
        );
    }
    let elided = interesting
        .len()
        .saturating_sub(policy.timeline_row_cap.get());
    if elided > 0 {
        push_fmt(
            out,
            format_args!("\n_… {elided} more interesting seconds elided._\n"),
        );
    }
    out.push('\n');
}

/// Timeline samples worth rendering: any second within
/// the configured window of an applied fault, plus any second whose committed
/// count deviates more than the configured threshold from the run's median, in
/// timeline order.
fn interesting_seconds_with_policy(
    report: &RunReport,
    policy: LoadtestRuntimePolicy,
) -> Vec<SecondSample> {
    let median = median_committed(&report.timeline);
    report
        .timeline
        .iter()
        .copied()
        .filter(|sample| {
            let near_fault = report
                .faults
                .iter()
                .any(|fault| (fault.at - sample.t).abs() <= policy.fault_window);
            near_fault
                || deviates_from_median_with_threshold(
                    sample.committed,
                    median,
                    policy.deviation_threshold,
                )
        })
        .collect()
}

/// Upper median of the per-second committed counts (0 for an empty timeline).
fn median_committed(timeline: &[SecondSample]) -> u64 {
    let mut counts: Vec<u64> = timeline.iter().map(|sample| sample.committed).collect();
    counts.sort_unstable();
    counts.get(counts.len() / 2).copied().unwrap_or(0)
}

/// True when `committed` deviates strictly more than the default threshold
/// from `median`. A zero median makes any non-zero count deviate (the
/// division is an infinity) and leaves an all-zero run boring (`NaN` compares
/// false).
#[cfg(test)]
fn deviates_from_median(committed: u64, median: u64) -> bool {
    deviates_from_median_with_threshold(
        committed,
        median,
        LoadtestRuntimePolicy::default().deviation_threshold,
    )
}

fn deviates_from_median_with_threshold(committed: u64, median: u64, threshold: Ratio) -> bool {
    fraction(u64_as_f64(committed.abs_diff(median)) / u64_as_f64(median)) > threshold
}

/// One row of the side-by-side comparison table, values pre-formatted and
/// the delta kept dimensioned for the notes line.
struct CompareRow {
    metric: String,
    left: String,
    right: String,
    delta: Option<Ratio>,
}

fn compare_rows(left: &RunReport, right: &RunReport) -> Vec<CompareRow> {
    let mut rows = vec![
        row_rate(
            "Mean TPS",
            left.throughput.mean_rate,
            right.throughput.mean_rate,
        ),
        row_u64(
            "Committed txn",
            left.throughput.committed_txn,
            right.throughput.committed_txn,
        ),
        row_u64(
            "Failed txn",
            left.throughput.failed_txn,
            right.throughput.failed_txn,
        ),
    ];
    for (class, left_latency) in &left.latency_by_class {
        if let Some(right_latency) = right.latency_by_class.get(class) {
            rows.push(row_time(
                format!("{class} p50"),
                left_latency.p50,
                right_latency.p50,
            ));
            rows.push(row_time(
                format!("{class} p99"),
                left_latency.p99,
                right_latency.p99,
            ));
        }
    }
    rows.push(row_u64(
        "Serialization retries",
        left.errors.serialization_retries,
        right.errors.serialization_retries,
    ));
    rows.push(row_u64(
        "Unavailable",
        left.errors.unavailable,
        right.errors.unavailable,
    ));
    rows.push(row_cpu(
        "Total CPU core-s",
        left.efficiency.total_cpu,
        right.efficiency.total_cpu,
    ));
    rows.push(row_rate(
        "Committed txn / CPU-s",
        left.efficiency.committed_txn_per_cpu,
        right.efficiency.committed_txn_per_cpu,
    ));
    rows.push(row_size("Peak RSS", peak_rss(left), peak_rss(right)));
    rows
}

fn row_rate(metric: impl Into<String>, left: Frequency, right: Frequency) -> CompareRow {
    CompareRow {
        metric: metric.into(),
        left: fmt2(left.per_sec_f64()),
        right: fmt2(right.per_sec_f64()),
        delta: relative_delta(left.per_sec_f64(), right.per_sec_f64()),
    }
}

fn row_cpu(metric: impl Into<String>, left: Time, right: Time) -> CompareRow {
    CompareRow {
        metric: metric.into(),
        left: fmt2(left.secs_f64()),
        right: fmt2(right.secs_f64()),
        delta: relative_delta(left.secs_f64(), right.secs_f64()),
    }
}

fn row_time(metric: impl Into<String>, left: Time, right: Time) -> CompareRow {
    CompareRow {
        metric: metric.into(),
        left: left.human().to_string(),
        right: right.human().to_string(),
        delta: relative_delta(left.secs_f64(), right.secs_f64()),
    }
}

fn row_u64(metric: impl Into<String>, left: u64, right: u64) -> CompareRow {
    CompareRow {
        metric: metric.into(),
        left: left.to_string(),
        right: right.to_string(),
        delta: relative_delta(u64_as_f64(left), u64_as_f64(right)),
    }
}

fn row_size(metric: impl Into<String>, left: ByteSize, right: ByteSize) -> CompareRow {
    CompareRow {
        metric: metric.into(),
        left: left.human().to_string(),
        right: right.human().to_string(),
        delta: relative_delta(left.bytes_f64(), right.bytes_f64()),
    }
}

/// Relative delta as a fraction of the left value, `None` when that value is
/// zero.
fn relative_delta(left: f64, right: f64) -> Option<Ratio> {
    (left != 0.0).then(|| fraction((right - left) / left))
}

/// Peak resident set size across all sampled processes.
fn peak_rss(report: &RunReport) -> ByteSize {
    report
        .resources
        .iter()
        .fold(ByteSize::ZERO, |peak, resources| {
            peak.max(resources.max_rss)
        })
}

/// Formats a value with two decimal places.
fn fmt2(value: f64) -> String {
    format!("{value:.2}")
}

/// Formats a relative delta as a signed percentage, `n/a` when undefined.
fn fmt_delta(delta: Option<Ratio>) -> String {
    delta.map_or_else(
        || "n/a".to_string(),
        |delta| format!("{:+.2}%", delta.percent_f64()),
    )
}

/// Formats an optional per-second mean latency, `-` when no operations
/// completed that second.
fn fmt_mean_latency(mean: Option<Time>) -> String {
    mean.map_or_else(|| "-".to_string(), |mean| mean.human().to_string())
}

/// Appends formatted text to `out`. Writing into a `String` cannot fail.
fn push_fmt(out: &mut String, args: fmt::Arguments<'_>) {
    out.write_fmt(args)
        .expect("writing to a String cannot fail");
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
    use assert2::{assert, check};

    use super::*;

    fn fixture(mode: &str) -> RunReport {
        let timeline = (0..=120)
            .map(|index| SecondSample {
                t: Time::from_secs(index),
                committed: if index == 31 { 10 } else { 100 },
                errors: if index == 31 { 3 } else { 0 },
                mean_latency: (index != 31).then(|| millis(1)),
            })
            .collect();
        let mut latency_by_class = BTreeMap::new();
        latency_by_class.insert(
            "single-shard-insert".to_string(),
            LatencySummary {
                count: 9000,
                mean: micros(1234),
                p50: micros(1100),
                p95: micros(2500),
                p99: micros(3750),
                p999: millis(5),
                max: micros(9987),
            },
        );
        latency_by_class.insert(
            "left-only-scan".to_string(),
            LatencySummary {
                count: 10,
                mean: millis(7),
                p50: millis(6),
                p95: millis(8),
                p99: millis(9),
                p999: micros(9500),
                max: micros(9900),
            },
        );
        RunReport {
            scenario: "steady-state".to_string(),
            description: "Steady mixed OLTP load.".to_string(),
            mode: mode.to_string(),
            started_unix_ms: 1_753_132_800_000,
            topology: TopologySummary {
                nodes: 3,
                ranges: 8,
            },
            duration: secs(120),
            throughput: ThroughputSummary {
                committed_txn: 12_000,
                failed_txn: 25,
                mean_rate: per_sec(1000),
            },
            latency_by_class,
            errors: ErrorSummary {
                serialization_retries: 0,
                unavailable: 4,
                connection_errors: 1,
                other: 2,
            },
            timeline,
            resources: vec![
                ProcessResources {
                    label: "broker".to_string(),
                    pid: 100,
                    cpu_time: millis(15_500),
                    max_rss: mebibytes(512),
                },
                ProcessResources {
                    label: "node0".to_string(),
                    pid: 200,
                    cpu_time: millis(24_500),
                    max_rss: mebibytes(256),
                },
            ],
            efficiency: EfficiencySummary {
                total_cpu: secs(40),
                committed_txn_per_cpu: per_sec(300),
            },
            faults: vec![AppliedFault {
                at: secs(30),
                description: "partition range:0 blackhole 10s".to_string(),
            }],
        }
    }

    #[test]
    fn explicit_policy_controls_timeline_selection_and_cap() {
        let policy = LoadtestRuntimePolicy {
            fault_window: secs(1),
            timeline_row_cap: crate::config::PositiveUsize::new(1).expect("cap"),
            deviation_threshold: percent(100),
            ..Default::default()
        };
        let rendered = render_markdown_with_policy(&fixture("logical-tso"), policy);
        assert!(rendered.contains("more interesting seconds elided"));
        assert!(!rendered.contains("| 25s |"));
    }

    /// The `hlc` counterpart of [`fixture`]: TPS +25% (the largest delta),
    /// only the shared latency class, a non-zero retry count against the
    /// left's zero, and no faults.
    fn right_fixture() -> RunReport {
        let mut report = fixture("hlc");
        report.throughput = ThroughputSummary {
            committed_txn: 12_600,
            failed_txn: 20,
            mean_rate: per_sec(1250),
        };
        report.errors = ErrorSummary {
            serialization_retries: 5,
            unavailable: 4,
            connection_errors: 0,
            other: 0,
        };
        report.efficiency = EfficiencySummary {
            total_cpu: secs(44),
            committed_txn_per_cpu: per_sec(300),
        };
        let mut latency_by_class = BTreeMap::new();
        latency_by_class.insert(
            "single-shard-insert".to_string(),
            LatencySummary {
                count: 9450,
                mean: micros(1300),
                p50: micros(1210),
                p95: micros(2600),
                p99: micros(3750),
                p999: micros(5200),
                max: micros(10_400),
            },
        );
        report.latency_by_class = latency_by_class;
        report.faults = Vec::new();
        report
    }

    #[test]
    fn markdown_headline_and_metadata() {
        let rendered = render_markdown(&fixture("logical-tso"));
        assert!(rendered.contains("# steady-state — logical-tso"));
        assert!(rendered.contains("Steady mixed OLTP load."));
        assert!(rendered.contains(
            "Started 1753132800000 unix ms — duration 120.00 s — topology 3 nodes / 8 ranges."
        ));
        assert!(rendered.contains("| 12000 | 25 | 1000.00 | 40.00 | 300.00 | 512MiB |"));
    }

    #[test]
    fn markdown_latency_errors_and_resources_rows() {
        let rendered = render_markdown(&fixture("logical-tso"));
        assert!(rendered.contains(
            "| single-shard-insert | 9000 | 1.234ms | 1.1ms | 2.5ms | 3.75ms | 5ms | 9.987ms |"
        ));
        assert!(rendered.contains("| 0 | 4 | 1 | 2 |"));
        assert!(rendered.contains("| broker | 100 | 15.50 | 512MiB |"));
        assert!(rendered.contains("| node0 | 200 | 24.50 | 256MiB |"));
    }

    #[test]
    fn markdown_timeline_selects_fault_window_and_outliers() {
        let rendered = render_markdown(&fixture("logical-tso"));
        assert!(rendered.contains("- t=30s — partition range:0 blackhole 10s"));
        // Fault-adjacent seconds (30 ± 5) are shown even at median throughput.
        assert!(rendered.contains("| 25s | 100 | 0 | 1ms |"));
        assert!(rendered.contains("| 28s | 100 | 0 | 1ms |"));
        assert!(rendered.contains("| 35s | 100 | 0 | 1ms |"));
        // The dip second deviates from the median and has no latency samples.
        assert!(rendered.contains("| 31s | 10 | 3 | - |"));
        // A boring second far from the fault is excluded, and nothing is
        // elided at 11 interesting seconds.
        assert!(!rendered.contains("| 1m30s |"));
        assert!(!rendered.contains("elided"));
    }

    #[test]
    fn markdown_timeline_caps_rows_with_elision_marker() {
        let mut report = fixture("logical-tso");
        report.faults = Vec::new();
        report.timeline = (0..200)
            .map(|index| SecondSample {
                t: Time::from_secs(index),
                committed: if index % 2 == 0 { 0 } else { 1000 },
                errors: 0,
                mean_latency: Some(millis(1)),
            })
            .collect();
        let rendered = render_markdown(&report);
        let section = rendered
            .split("## Timeline")
            .nth(1)
            .expect("timeline section");
        let rows = section.lines().filter(|line| line.starts_with('|')).count();
        assert!(rows == 62, "header + separator + 60 data rows, got {rows}");
        assert!(section.contains("_… 40 more interesting seconds elided._"));
    }

    #[test]
    fn markdown_skips_empty_timeline_and_reports_no_faults() {
        let mut report = fixture("logical-tso");
        report.timeline = Vec::new();
        report.faults = Vec::new();
        let rendered = render_markdown(&report);
        assert!(!rendered.contains("## Timeline"));
        assert!(rendered.contains("## Faults\n\nnone"));
    }

    #[test]
    fn comparison_deltas_and_notes() {
        let rendered = render_comparison(&fixture("logical-tso"), &right_fixture());
        assert!(rendered.contains("# steady-state — logical-tso vs hlc"));
        assert!(rendered.contains("| Mean TPS | 1000.00 | 1250.00 | +25.00% |"));
        assert!(rendered.contains("| Committed txn | 12000 | 12600 | +5.00% |"));
        assert!(rendered.contains("| Failed txn | 25 | 20 | -20.00% |"));
        assert!(rendered.contains("| single-shard-insert p50 | 1.1ms | 1.21ms | +10.00% |"));
        assert!(rendered.contains("| Serialization retries | 0 | 5 | n/a |"));
        assert!(rendered.contains("| Peak RSS | 512MiB | 512MiB | +0.00% |"));
        // Classes present in only one run are not compared.
        assert!(!rendered.contains("left-only-scan"));
        assert!(rendered.contains("Notes: largest delta — Mean TPS (+25.00%)."));
    }

    #[test]
    fn comparison_lists_both_fault_logs() {
        let rendered = render_comparison(&fixture("logical-tso"), &right_fixture());
        assert!(
            rendered
                .contains("## Faults — logical-tso\n\n- t=30s — partition range:0 blackhole 10s")
        );
        assert!(rendered.contains("## Faults — hlc\n\nnone"));
    }

    #[test]
    fn format_helpers() {
        check!(fmt2(1234.567) == "1234.57");
        check!(fmt2(0.0) == "0.00");
        check!(mebibytes(512).human().to_string() == "512MiB");
        check!(ByteSize::ZERO.human().to_string() == "0B");
        check!(fmt_delta(None) == "n/a");
        check!(fmt_delta(Some(percent(25))) == "+25.00%");
        check!(fmt_delta(Some(-percent(12) - fraction(0.005))) == "-12.50%");
        check!(fmt_mean_latency(None) == "-");
        check!(fmt_mean_latency(Some(micros(1234))) == "1.234ms");
    }

    #[test]
    fn deviation_rule_is_strictly_more_than_the_threshold() {
        let cases = [
            (100, 100, false),
            (130, 100, false),
            (131, 100, true),
            (70, 100, false),
            (69, 100, true),
            (1, 0, true),
            (0, 0, false),
        ];
        for (committed, median, expected) in cases {
            check!(
                deviates_from_median(committed, median) == expected,
                "committed: {committed}, median: {median}"
            );
        }
    }

    #[test]
    fn run_report_json_round_trip() {
        let report = fixture("logical-tso");
        let json = serde_json::to_string(&report).expect("serialize");
        let back: RunReport = serde_json::from_str(&json).expect("deserialize");
        assert!(back == report);
    }

    #[test]
    fn quantity_fields_encode_in_their_declared_units() {
        // Human-facing fields carry their unit; compared and plotted fields
        // are exact integers in a named unit.
        let report = fixture("logical-tso");
        let json = serde_json::to_value(&report).expect("serialize");
        let cases: [(&str, serde_json::Value); 6] = [
            ("duration (ms)", json["duration"].clone()),
            (
                "latency p50 (ns)",
                json["latency_by_class"]["single-shard-insert"]["p50"].clone(),
            ),
            ("timeline t (s)", json["timeline"][30]["t"].clone()),
            (
                "timeline mean latency (ns)",
                json["timeline"][30]["mean_latency"].clone(),
            ),
            ("mean rate (human)", json["throughput"]["mean_rate"].clone()),
            ("peak RSS (human)", json["resources"][0]["max_rss"].clone()),
        ];
        let expected: [serde_json::Value; 6] = [
            serde_json::json!(120_000),
            serde_json::json!(1_100_000),
            serde_json::json!(30),
            serde_json::json!(1_000_000),
            serde_json::json!("1000/s"),
            serde_json::json!("512MiB"),
        ];
        for ((label, actual), expected) in cases.into_iter().zip(expected) {
            check!(actual == expected, "{label}");
        }
    }
}
