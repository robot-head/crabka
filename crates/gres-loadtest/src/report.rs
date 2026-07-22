//! Serialisable run results and their Markdown rendering.
//!
//! [`RunReport`] is the harness's durable output: written as JSON next to a
//! rendered Markdown summary. [`render_comparison`] lines up two reports of
//! the same scenario under different timestamp modes.

use std::{
    collections::BTreeMap,
    fmt::{self, Write as _},
};

use serde::{Deserialize, Serialize};

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
    /// Measured window length in seconds (excludes warmup).
    pub duration_s: f64,
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
    /// Mean committed transactions per second.
    pub tps_mean: f64,
}

/// Latency distribution for one operation class, in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    /// Operations measured.
    pub count: u64,
    /// Mean latency.
    pub mean_ms: f64,
    /// 50th percentile.
    pub p50_ms: f64,
    /// 95th percentile.
    pub p95_ms: f64,
    /// 99th percentile.
    pub p99_ms: f64,
    /// 99.9th percentile.
    pub p999_ms: f64,
    /// Maximum observed.
    pub max_ms: f64,
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
    /// Seconds since the measurement window started.
    pub t_s: u64,
    /// Transactions committed during this second.
    pub committed: u64,
    /// Errors observed during this second.
    pub errors: u64,
    /// Mean latency of operations completed this second, if any.
    pub mean_latency_ms: Option<f64>,
}

/// Resource usage of one launched process over the measurement window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessResources {
    /// Process label (`broker`, `node0`, ...).
    pub label: String,
    /// OS process id.
    pub pid: u32,
    /// CPU consumed, in core-seconds (user + system).
    pub cpu_core_seconds: f64,
    /// Peak resident set size observed, in bytes.
    pub max_rss_bytes: u64,
}

/// Cluster-wide efficiency derived from throughput and resources.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EfficiencySummary {
    /// Total CPU consumed by broker + nodes, in core-seconds.
    pub total_cpu_core_seconds: f64,
    /// Committed transactions per consumed CPU core-second.
    pub committed_txn_per_cpu_second: f64,
}

/// A fault the scheduler actually applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedFault {
    /// Seconds after the measurement window started.
    pub at_s: u64,
    /// Human-readable description (e.g. `partition range:0 blackhole 10s`).
    pub description: String,
}

/// Seconds on either side of an applied fault whose timeline rows are always
/// rendered.
const FAULT_WINDOW_S: u64 = 5;

/// Maximum number of timeline rows rendered before eliding the rest.
const TIMELINE_ROW_CAP: usize = 60;

/// Bytes per mebibyte, for human-readable RSS values.
const BYTES_PER_MIB: f64 = 1_048_576.0;

/// Renders one run as a Markdown summary: headline throughput/efficiency,
/// per-class latency table, error table, per-process resource table, fault
/// log, and a compact per-second table of the interesting seconds (those
/// near an applied fault or whose committed count deviates more than 30%
/// from the run's median).
#[must_use]
pub fn render_markdown(report: &RunReport) -> String {
    let mut out = String::new();
    push_header(&mut out, report);
    push_headline(&mut out, report);
    push_latency(&mut out, report);
    push_errors(&mut out, report);
    push_resources(&mut out, report);
    push_faults(&mut out, report);
    push_timeline(&mut out, report);
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
        .filter_map(|row| row.delta.map(|delta| (delta.abs(), row)))
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
            fmt2(report.duration_s),
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
            fmt2(report.throughput.tps_mean),
            fmt2(report.efficiency.total_cpu_core_seconds),
            fmt2(report.efficiency.committed_txn_per_cpu_second),
            fmt_mib(peak_rss_bytes(report)),
        ),
    );
}

fn push_latency(out: &mut String, report: &RunReport) {
    out.push_str("## Latency by class (ms)\n\n");
    out.push_str("| Class | Count | Mean | p50 | p95 | p99 | p99.9 | Max |\n");
    out.push_str("| :-- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for (class, latency) in &report.latency_by_class {
        push_fmt(
            out,
            format_args!(
                "| {class} | {} | {} | {} | {} | {} | {} | {} |\n",
                latency.count,
                fmt2(latency.mean_ms),
                fmt2(latency.p50_ms),
                fmt2(latency.p95_ms),
                fmt2(latency.p99_ms),
                fmt2(latency.p999_ms),
                fmt2(latency.max_ms),
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
                fmt2(resources.cpu_core_seconds),
                fmt_mib(resources.max_rss_bytes),
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
            format_args!("- t={}s — {}\n", fault.at_s, fault.description),
        );
    }
}

fn push_timeline(out: &mut String, report: &RunReport) {
    if report.timeline.is_empty() {
        return;
    }
    out.push_str("## Timeline (interesting seconds)\n\n");
    let interesting = interesting_seconds(report);
    if interesting.is_empty() {
        push_fmt(
            out,
            format_args!(
                "No interesting seconds — committed stayed within 30% of the median across {} \
             samples and no faults applied.\n\n",
                report.timeline.len()
            ),
        );
        return;
    }
    out.push_str("| t (s) | Committed | Errors | Mean latency (ms) |\n");
    out.push_str("| ---: | ---: | ---: | ---: |\n");
    for sample in interesting.iter().take(TIMELINE_ROW_CAP) {
        push_fmt(
            out,
            format_args!(
                "| {} | {} | {} | {} |\n",
                sample.t_s,
                sample.committed,
                sample.errors,
                fmt_mean_latency(sample.mean_latency_ms),
            ),
        );
    }
    let elided = interesting.len().saturating_sub(TIMELINE_ROW_CAP);
    if elided > 0 {
        push_fmt(
            out,
            format_args!("\n_… {elided} more interesting seconds elided._\n"),
        );
    }
    out.push('\n');
}

/// Timeline samples worth rendering: any second within
/// ±[`FAULT_WINDOW_S`] of an applied fault, plus any second whose committed
/// count deviates more than 30% from the run's median, in timeline order.
fn interesting_seconds(report: &RunReport) -> Vec<SecondSample> {
    let median = median_committed(&report.timeline);
    report
        .timeline
        .iter()
        .copied()
        .filter(|sample| {
            let near_fault = report
                .faults
                .iter()
                .any(|fault| fault.at_s.abs_diff(sample.t_s) <= FAULT_WINDOW_S);
            near_fault || deviates_from_median(sample.committed, median)
        })
        .collect()
}

/// Upper median of the per-second committed counts (0 for an empty timeline).
fn median_committed(timeline: &[SecondSample]) -> u64 {
    let mut counts: Vec<u64> = timeline.iter().map(|sample| sample.committed).collect();
    counts.sort_unstable();
    counts.get(counts.len() / 2).copied().unwrap_or(0)
}

/// True when `committed` deviates strictly more than 30% from `median`,
/// computed in integers to stay exact.
fn deviates_from_median(committed: u64, median: u64) -> bool {
    u128::from(committed.abs_diff(median)) * 10 > u128::from(median) * 3
}

/// One row of the side-by-side comparison table, values pre-formatted and
/// the delta kept numeric for the notes line.
struct CompareRow {
    metric: String,
    left: String,
    right: String,
    delta: Option<f64>,
}

fn compare_rows(left: &RunReport, right: &RunReport) -> Vec<CompareRow> {
    let mut rows = vec![
        row_f64(
            "Mean TPS",
            left.throughput.tps_mean,
            right.throughput.tps_mean,
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
            rows.push(row_f64(
                format!("{class} p50 (ms)"),
                left_latency.p50_ms,
                right_latency.p50_ms,
            ));
            rows.push(row_f64(
                format!("{class} p99 (ms)"),
                left_latency.p99_ms,
                right_latency.p99_ms,
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
    rows.push(row_f64(
        "Total CPU core-s",
        left.efficiency.total_cpu_core_seconds,
        right.efficiency.total_cpu_core_seconds,
    ));
    rows.push(row_f64(
        "Committed txn / CPU-s",
        left.efficiency.committed_txn_per_cpu_second,
        right.efficiency.committed_txn_per_cpu_second,
    ));
    rows.push(row_mib(
        "Peak RSS",
        peak_rss_bytes(left),
        peak_rss_bytes(right),
    ));
    rows
}

fn row_f64(metric: impl Into<String>, left: f64, right: f64) -> CompareRow {
    CompareRow {
        metric: metric.into(),
        left: fmt2(left),
        right: fmt2(right),
        delta: delta_pct(left, right),
    }
}

fn row_u64(metric: impl Into<String>, left: u64, right: u64) -> CompareRow {
    CompareRow {
        metric: metric.into(),
        left: left.to_string(),
        right: right.to_string(),
        delta: delta_pct(u64_as_f64(left), u64_as_f64(right)),
    }
}

fn row_mib(metric: impl Into<String>, left_bytes: u64, right_bytes: u64) -> CompareRow {
    CompareRow {
        metric: metric.into(),
        left: fmt_mib(left_bytes),
        right: fmt_mib(right_bytes),
        delta: delta_pct(u64_as_f64(left_bytes), u64_as_f64(right_bytes)),
    }
}

/// Relative delta in percent, `None` when the left value is zero.
fn delta_pct(left: f64, right: f64) -> Option<f64> {
    if left == 0.0 {
        None
    } else {
        Some((right - left) / left * 100.0)
    }
}

/// Peak resident set size across all sampled processes, in bytes.
fn peak_rss_bytes(report: &RunReport) -> u64 {
    report
        .resources
        .iter()
        .map(|resources| resources.max_rss_bytes)
        .max()
        .unwrap_or(0)
}

/// Formats a value with two decimal places.
fn fmt2(value: f64) -> String {
    format!("{value:.2}")
}

/// Formats a byte count as mebibytes with one decimal place.
fn fmt_mib(bytes: u64) -> String {
    format!("{:.1} MiB", u64_as_f64(bytes) / BYTES_PER_MIB)
}

/// Formats a percentage delta with an explicit sign, `n/a` when undefined.
fn fmt_delta(delta: Option<f64>) -> String {
    delta.map_or_else(|| "n/a".to_string(), |delta| format!("{delta:+.2}%"))
}

/// Formats an optional per-second mean latency, `-` when no operations
/// completed that second.
fn fmt_mean_latency(mean_ms: Option<f64>) -> String {
    mean_ms.map_or_else(|| "-".to_string(), fmt2)
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
    use assert2::assert;

    use super::*;

    fn fixture(mode: &str) -> RunReport {
        let timeline = (0..=120)
            .map(|t_s| SecondSample {
                t_s,
                committed: if t_s == 31 { 10 } else { 100 },
                errors: if t_s == 31 { 3 } else { 0 },
                mean_latency_ms: if t_s == 31 { None } else { Some(1.0) },
            })
            .collect();
        let mut latency_by_class = BTreeMap::new();
        latency_by_class.insert(
            "single-shard-insert".to_string(),
            LatencySummary {
                count: 9000,
                mean_ms: 1.234,
                p50_ms: 1.1,
                p95_ms: 2.5,
                p99_ms: 3.75,
                p999_ms: 5.0,
                max_ms: 9.987,
            },
        );
        latency_by_class.insert(
            "left-only-scan".to_string(),
            LatencySummary {
                count: 10,
                mean_ms: 7.0,
                p50_ms: 6.0,
                p95_ms: 8.0,
                p99_ms: 9.0,
                p999_ms: 9.5,
                max_ms: 9.9,
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
            duration_s: 120.0,
            throughput: ThroughputSummary {
                committed_txn: 12_000,
                failed_txn: 25,
                tps_mean: 1000.0,
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
                    cpu_core_seconds: 15.5,
                    max_rss_bytes: 536_870_912,
                },
                ProcessResources {
                    label: "node0".to_string(),
                    pid: 200,
                    cpu_core_seconds: 24.5,
                    max_rss_bytes: 268_435_456,
                },
            ],
            efficiency: EfficiencySummary {
                total_cpu_core_seconds: 40.0,
                committed_txn_per_cpu_second: 300.0,
            },
            faults: vec![AppliedFault {
                at_s: 30,
                description: "partition range:0 blackhole 10s".to_string(),
            }],
        }
    }

    /// The `hlc` counterpart of [`fixture`]: TPS +25% (the largest delta),
    /// only the shared latency class, a non-zero retry count against the
    /// left's zero, and no faults.
    fn right_fixture() -> RunReport {
        let mut report = fixture("hlc");
        report.throughput = ThroughputSummary {
            committed_txn: 12_600,
            failed_txn: 20,
            tps_mean: 1250.0,
        };
        report.errors = ErrorSummary {
            serialization_retries: 5,
            unavailable: 4,
            connection_errors: 0,
            other: 0,
        };
        report.efficiency = EfficiencySummary {
            total_cpu_core_seconds: 44.0,
            committed_txn_per_cpu_second: 300.0,
        };
        let mut latency_by_class = BTreeMap::new();
        latency_by_class.insert(
            "single-shard-insert".to_string(),
            LatencySummary {
                count: 9450,
                mean_ms: 1.3,
                p50_ms: 1.21,
                p95_ms: 2.6,
                p99_ms: 3.75,
                p999_ms: 5.2,
                max_ms: 10.4,
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
        assert!(rendered.contains("| 12000 | 25 | 1000.00 | 40.00 | 300.00 | 512.0 MiB |"));
    }

    #[test]
    fn markdown_latency_errors_and_resources_rows() {
        let rendered = render_markdown(&fixture("logical-tso"));
        assert!(
            rendered.contains(
                "| single-shard-insert | 9000 | 1.23 | 1.10 | 2.50 | 3.75 | 5.00 | 9.99 |"
            )
        );
        assert!(rendered.contains("| 0 | 4 | 1 | 2 |"));
        assert!(rendered.contains("| broker | 100 | 15.50 | 512.0 MiB |"));
        assert!(rendered.contains("| node0 | 200 | 24.50 | 256.0 MiB |"));
    }

    #[test]
    fn markdown_timeline_selects_fault_window_and_outliers() {
        let rendered = render_markdown(&fixture("logical-tso"));
        assert!(rendered.contains("- t=30s — partition range:0 blackhole 10s"));
        // Fault-adjacent seconds (30 ± 5) are shown even at median throughput.
        assert!(rendered.contains("| 25 | 100 | 0 | 1.00 |"));
        assert!(rendered.contains("| 28 | 100 | 0 | 1.00 |"));
        assert!(rendered.contains("| 35 | 100 | 0 | 1.00 |"));
        // The dip second deviates from the median and has no latency samples.
        assert!(rendered.contains("| 31 | 10 | 3 | - |"));
        // A boring second far from the fault is excluded, and nothing is
        // elided at 11 interesting seconds.
        assert!(!rendered.contains("| 90 |"));
        assert!(!rendered.contains("elided"));
    }

    #[test]
    fn markdown_timeline_caps_rows_with_elision_marker() {
        let mut report = fixture("logical-tso");
        report.faults = Vec::new();
        report.timeline = (0..200)
            .map(|t_s| SecondSample {
                t_s,
                committed: if t_s % 2 == 0 { 0 } else { 1000 },
                errors: 0,
                mean_latency_ms: Some(1.0),
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
        assert!(rendered.contains("| single-shard-insert p50 (ms) | 1.10 | 1.21 | +10.00% |"));
        assert!(rendered.contains("| Serialization retries | 0 | 5 | n/a |"));
        assert!(rendered.contains("| Peak RSS | 512.0 MiB | 512.0 MiB | +0.00% |"));
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
        assert!(fmt2(1234.567) == "1234.57");
        assert!(fmt2(0.0) == "0.00");
        assert!(fmt_mib(536_870_912) == "512.0 MiB");
        assert!(fmt_mib(0) == "0.0 MiB");
        assert!(fmt_delta(None) == "n/a");
        assert!(fmt_delta(Some(25.0)) == "+25.00%");
        assert!(fmt_delta(Some(-12.5)) == "-12.50%");
        assert!(fmt_mean_latency(None) == "-");
        assert!(fmt_mean_latency(Some(1.234)) == "1.23");
    }

    #[test]
    fn deviation_rule_is_strictly_more_than_30_percent() {
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
            assert!(
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
}
