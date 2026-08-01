//! Aggregate per-run JSON outputs into a single Markdown summary.

use std::{
    collections::BTreeMap,
    fmt::{Arguments, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use crabka_units::{fmt::Human as _, prelude::*};

use crate::{
    numeric::{mebibytes_f64, millis_f64, to_f64},
    scenario::{RunOutput, Stack},
};

fn push_fmt(output: &mut String, args: Arguments<'_>) {
    output
        .write_fmt(args)
        .expect("writing formatted data to a String cannot fail");
}

/// How a metric's cross-run mean is written into a table cell.
///
/// A mean and a standard deviation are dimension-agnostic, so the statistics
/// below run over plain numbers in one fixed unit per metric; this says which
/// quantity to rebuild the mean into so the cell carries its unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Render {
    /// A dimensionless number (a count, a rate of dimensionless things).
    Number,
    /// A time extent, from a sample of seconds.
    Extent,
    /// A byte count, from a sample of bytes.
    Size,
    /// A byte throughput, from a sample of bytes per second.
    Throughput,
    /// An event rate, from a sample of events per second.
    Rate,
}

impl Render {
    /// The mean of `values` in this metric's unit.
    fn mean(self, values: &[f64]) -> String {
        let m = mean(values);
        match self {
            Render::Number => format!("{m:.3}"),
            Render::Extent => Time::from_secs_f64(m).human().to_string(),
            Render::Size => ByteSize::from_bytes_f64(m).human().to_string(),
            Render::Throughput => ByteRate::from_bytes_per_sec_f64(m).human().to_string(),
            Render::Rate => Frequency::from_per_sec(m).human().to_string(),
        }
    }

    /// Render per-run samples as `mean` (single run) or `mean (±cv%)` (multiple
    /// runs), where cv is the coefficient of variation.
    fn cell(self, values: &[f64]) -> String {
        let rendered = self.mean(values);
        if values.len() > 1 {
            let m = mean(values);
            let cv = if m == 0.0 {
                0.0
            } else {
                (sample_stddev(values) / m).abs() * 100.0
            };
            format!("{rendered} (±{cv:.0}%)")
        } else {
            rendered
        }
    }
}

/// Walk `input_dir` for `*.json` files, deserialize each into a
/// `RunOutput`, group by `(scenario name, broker_count)`, average each
/// metric across all runs in a group, and emit a Markdown summary.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub fn render_markdown(input_dir: &Path, strict: bool) -> Result<String> {
    let mut runs: Vec<(PathBuf, RunOutput)> = Vec::new();
    let entries = std::fs::read_dir(input_dir)
        .with_context(|| format!("read_dir {}", input_dir.display()))?;
    for e in entries {
        let e = e.context("dir entry")?;
        let path = e.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let body =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        match serde_json::from_str::<RunOutput>(&body) {
            Ok(r) => runs.push((path, r)),
            Err(e) => {
                if strict {
                    return Err(anyhow::anyhow!("failed to parse {}: {e}", path.display()));
                }
                eprintln!("warn: skipping unparseable {}: {e}", path.display());
            }
        }
    }

    // Group by (scenario name, broker_count). Keying on broker_count keeps the
    // same scenario run at two topologies (e.g. 3- vs 6-broker) in separate
    // cells, and collects every repeated run of one cell together to average.
    let mut by_group: BTreeMap<(String, u32), Vec<RunOutput>> = BTreeMap::new();
    for (_p, r) in runs {
        by_group
            .entry((r.scenario.name.clone(), r.topology.broker_count))
            .or_default()
            .push(r);
    }

    let mut out = String::new();
    out.push_str("# Crabka vs Strimzi benchmark — results\n\n");
    if by_group.is_empty() {
        out.push_str("_no `RunOutput` JSON files found in input dir_\n");
        return Ok(out);
    }
    out.push_str("Each cell is the **mean across all runs** of a (scenario, topology); `(±N%)` is the coefficient of variation (sample stddev ÷ mean), shown when a cell has more than one run. The `ratio` column is `crabka / kafka` for throughput / efficiency (higher is better for Crabka) and `kafka / crabka` for latency / resource (lower-is-better Crabka still > 1).\n\n");

    for ((name, brokers), runs) in &by_group {
        render_group(&mut out, name, *brokers, runs);
    }

    Ok(out)
}

fn render_group(out: &mut String, name: &str, brokers: u32, runs: &[RunOutput]) {
    let crabka: Vec<&RunOutput> = runs
        .iter()
        .filter(|r| matches!(r.stack, Stack::Crabka))
        .collect();
    let kafka: Vec<&RunOutput> = runs
        .iter()
        .filter(|r| matches!(r.stack, Stack::Kafka))
        .collect();

    push_fmt(out, format_args!("## `{name}` @ {brokers} broker(s)\n\n"));

    if let Some(r) = runs.first() {
        push_fmt(
            out,
            format_args!(
                "Topology: partitions={}, RF={}, broker_count={} (per stack). Duration={}, warmup={}. Runs averaged: crabka={}, kafka={}.\n\n",
                r.topology.partitions,
                r.topology.replication_factor,
                r.topology.broker_count,
                r.scenario.duration.human(),
                r.scenario.warmup.human(),
                crabka.len(),
                kafka.len(),
            ),
        );
    }

    // ── Topline table ───────────────────────────────────────────────────
    out.push_str("| metric | crabka | kafka | ratio |\n");
    out.push_str("|---|---|---|---|\n");
    row_metric(
        out,
        "producer msgs/s (higher better)",
        (&crabka, &kafka),
        (|t| t.throughput.producer_rate.per_sec_f64(), Render::Rate),
        true,
    );
    row_metric(
        out,
        "consumer msgs/s (higher better)",
        (&crabka, &kafka),
        (|t| t.throughput.consumer_rate.per_sec_f64(), Render::Rate),
        true,
    );
    row_metric(
        out,
        "producer byte rate (higher better)",
        (&crabka, &kafka),
        (
            |t| producer_byte_rate(t).bytes_per_sec_f64(),
            Render::Throughput,
        ),
        true,
    );
    row_metric(
        out,
        "p99 producer ack (lower better)",
        (&crabka, &kafka),
        (|t| t.producer_latency.p99.secs_f64(), Render::Extent),
        false,
    );
    row_metric(
        out,
        "p99 consumer e2e (lower better)",
        (&crabka, &kafka),
        (|t| t.consumer_e2e_latency.p99.secs_f64(), Render::Extent),
        false,
    );
    row_metric(
        out,
        "msgs/s per CPU-core (higher better)",
        (&crabka, &kafka),
        (
            |t| t.resource.msgs_per_cpu_second.per_sec_f64(),
            Render::Rate,
        ),
        true,
    );
    row_metric(
        out,
        "cgroup working set (lower better)",
        (&crabka, &kafka),
        (
            |t| t.resource.mem_cgroup_working_set.bytes_f64(),
            Render::Size,
        ),
        false,
    );
    row_metric(
        out,
        "startup (CR-apply → Ready) (lower better)",
        (&crabka, &kafka),
        (
            |t| t.startup.unwrap_or(Time::ZERO).secs_f64(),
            Render::Extent,
        ),
        false,
    );
    row_metric(
        out,
        "first ack (Ready → first ack) (lower better)",
        (&crabka, &kafka),
        (|t| t.first_ack.secs_f64(), Render::Extent),
        false,
    );
    out.push('\n');

    // ── Latency percentiles (mean across runs) ──────────────────────────
    render_latency_table(out, "**Producer ack latency:**", (&crabka, &kafka), |r| {
        &r.producer_latency
    });
    render_latency_table(
        out,
        "**Consumer end-to-end latency:**",
        (&crabka, &kafka),
        |r| &r.consumer_e2e_latency,
    );

    // ── Kafka-only memory split (mean across kafka runs that report it) ──
    let heap: Vec<f64> = kafka
        .iter()
        .filter_map(|r| r.resource.jvm_heap_used)
        .map(ByteSizeExt::bytes_f64)
        .collect();
    let nonheap: Vec<f64> = kafka
        .iter()
        .filter_map(|r| r.resource.jvm_nonheap_used)
        .map(ByteSizeExt::bytes_f64)
        .collect();
    let pc: Vec<f64> = kafka
        .iter()
        .filter_map(|r| r.resource.kafka_page_cache_approx)
        .map(ByteSizeExt::bytes_f64)
        .collect();
    if !heap.is_empty() && !nonheap.is_empty() && !pc.is_empty() {
        let ws: Vec<f64> = kafka
            .iter()
            .map(|r| r.resource.mem_cgroup_working_set.bytes_f64())
            .collect();
        out.push_str("**Kafka memory split:**\n\n");
        push_fmt(
            out,
            format_args!(
                "- JVM heap used: {}\n- JVM non-heap used: {}\n- Page-cache (approx, working-set − heap − non-heap): {}\n- cgroup working-set (limit-relevant): {}\n\n",
                Render::Size.mean(&heap),
                Render::Size.mean(&nonheap),
                Render::Size.mean(&pc),
                Render::Size.mean(&ws),
            ),
        );
    }

    // ── Failover disturbance (mean across runs that injected a kill) ────
    for (label, stack_runs) in [("crabka", &crabka), ("kafka", &kafka)] {
        let dists: Vec<&crate::scenario::Disturbance> = stack_runs
            .iter()
            .filter_map(|r| r.disturbance.as_ref())
            .collect();
        if dists.is_empty() {
            continue;
        }
        let recovery: Vec<f64> = dists
            .iter()
            .map(|d| d.recovery_at_ms.since(d.kill_at_ms).secs_f64())
            .collect();
        let dropped: Vec<f64> = dists.iter().map(|d| to_f64(d.dropped.0)).collect();
        let spike: Vec<f64> = dists
            .iter()
            .map(|d| d.latency_spike_max.secs_f64())
            .collect();
        push_fmt(
            out,
            format_args!(
                "**Failover ({label}, n={}):** recovery {}, {:.0} drops, max latency spike {}.\n\n",
                dists.len(),
                Render::Extent.mean(&recovery),
                mean(&dropped),
                Render::Extent.mean(&spike),
            ),
        );
    }
    render_failover_comparison(out, &crabka, &kafka);

    render_notes_and_errors(out, &crabka, &kafka);
    out.push('\n');
}

fn render_notes_and_errors(out: &mut String, crabka: &[&RunOutput], kafka: &[&RunOutput]) {
    for (label, stack_runs) in [("crabka", crabka), ("kafka", kafka)] {
        let mut notes: Vec<String> = Vec::new();
        let mut errors: Vec<String> = Vec::new();
        for r in stack_runs {
            for n in &r.notes {
                if !notes.contains(n) {
                    notes.push(n.clone());
                }
            }
            for e in &r.errors {
                if !errors.contains(e) {
                    errors.push(e.clone());
                }
            }
        }
        if !notes.is_empty() {
            push_fmt(
                out,
                format_args!("_{label} notes:_ {}\n\n", notes.join(", ")),
            );
        }
        if !errors.is_empty() {
            push_fmt(
                out,
                format_args!("_{label} errors:_ {}\n\n", truncate_list(&errors, 3)),
            );
        }
    }
}

/// Return human-readable failover gate violations. An empty vector means every
/// failover cell with both stacks present has the evidence needed for the
/// objective: Crabka recovered no slower than Kafka, and both stacks emitted
/// producer and consumer message-rate samples over time.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub fn failover_gate_violations(input_dir: &Path, strict: bool) -> Result<Vec<String>> {
    let runs: Vec<RunOutput> = collect_runs(input_dir, strict)?
        .into_iter()
        .map(|(_, r)| r)
        .collect();
    Ok(failover_gate_violations_for_runs(&runs))
}

fn truncate_list(items: &[String], n: usize) -> String {
    let head: Vec<_> = items.iter().take(n).cloned().collect();
    let extra = items.len().saturating_sub(n);
    if extra > 0 {
        format!("{} (+{extra} more)", head.join("; "))
    } else {
        head.join("; ")
    }
}

type LatencySelector = fn(&crate::scenario::LatencyPercentiles) -> f64;

/// The percentile rows of a latency table: a label, how to read the value out in
/// the unit the statistics run over, and how to write the mean back.
fn latency_percentiles_pairs() -> [(&'static str, LatencySelector, Render); 7] {
    [
        ("p50", |p| p.p50.secs_f64(), Render::Extent),
        ("p95", |p| p.p95.secs_f64(), Render::Extent),
        ("p99", |p| p.p99.secs_f64(), Render::Extent),
        ("p99.9", |p| p.p999.secs_f64(), Render::Extent),
        ("max", |p| p.max.secs_f64(), Render::Extent),
        ("mean", |p| p.mean.secs_f64(), Render::Extent),
        ("count", |p| to_f64(p.count), Render::Number),
    ]
}

/// One stack-vs-stack latency percentile table.
fn render_latency_table(
    out: &mut String,
    heading: &str,
    (crabka, kafka): (&[&RunOutput], &[&RunOutput]),
    select: fn(&RunOutput) -> &crate::scenario::LatencyPercentiles,
) {
    push_fmt(out, format_args!("{heading}\n\n"));
    out.push_str("| percentile | crabka | kafka |\n|---|---|---|\n");
    for (label, value, render) in latency_percentiles_pairs() {
        let c: Vec<f64> = crabka.iter().map(|r| value(select(r))).collect();
        let k: Vec<f64> = kafka.iter().map(|r| value(select(r))).collect();
        push_fmt(
            out,
            format_args!("| {label} | {} | {} |\n", render.cell(&c), render.cell(&k)),
        );
    }
    out.push('\n');
}

/// The rate at which a run pushed record bytes over its measurement window.
fn producer_byte_rate(r: &RunOutput) -> ByteRate {
    let window = if r.scenario.duration > Time::ZERO {
        r.scenario.duration
    } else {
        secs(1)
    };
    byte_rate(r.throughput.bytes_in, window)
}

/// Arithmetic mean of a sample. Empty sample → 0.0 (an absent stack renders
/// as a zero cell, matching the pre-averaging behaviour).
fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / to_f64(v.len())
    }
}

/// Sample standard deviation (Bessel-corrected). Zero for fewer than two
/// samples — a single run has no measurable spread.
fn sample_stddev(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    let m = mean(v);
    let var = v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (to_f64(v.len()) - 1.0);
    var.sqrt()
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RateRecovery {
    baseline: Frequency,
    min_after_kill: Frequency,
    recovery: Option<Time>,
}

fn render_failover_comparison(out: &mut String, crabka: &[&RunOutput], kafka: &[&RunOutput]) {
    let c_recovery = mean_failover_recovery(crabka);
    let k_recovery = mean_failover_recovery(kafka);
    let (Some(c_recovery), Some(k_recovery)) = (c_recovery, k_recovery) else {
        return;
    };

    let verdict = if c_recovery <= k_recovery {
        "PASS"
    } else {
        "FAIL"
    };
    let delta = if c_recovery <= k_recovery {
        k_recovery - c_recovery
    } else {
        c_recovery - k_recovery
    };
    let faster = if c_recovery <= k_recovery {
        "faster than"
    } else {
        "slower than"
    };
    push_fmt(
        out,
        format_args!(
            "**Failover comparison:** {verdict} — Crabka recovered {} {faster} kafka (crabka {}, kafka {}).\n\n",
            delta.human(),
            c_recovery.human(),
            k_recovery.human(),
        ),
    );

    let c_producer_rate = mean_rate_recovery(crabka, producer_sample_rate);
    let k_producer_rate = mean_rate_recovery(kafka, producer_sample_rate);
    let c_consumer_rate = mean_rate_recovery(crabka, consumer_sample_rate);
    let k_consumer_rate = mean_rate_recovery(kafka, consumer_sample_rate);
    if c_producer_rate.is_none()
        && k_producer_rate.is_none()
        && c_consumer_rate.is_none()
        && k_consumer_rate.is_none()
    {
        return;
    }

    out.push_str("| stack | recovery ms | producer baseline msgs/s | producer min after kill msgs/s | producer rate recovery ms | consumer baseline msgs/s | consumer min after kill msgs/s | consumer rate recovery ms |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|\n");
    render_rate_recovery_row(out, "crabka", c_recovery, c_producer_rate, c_consumer_rate);
    render_rate_recovery_row(out, "kafka", k_recovery, k_producer_rate, k_consumer_rate);
    out.push('\n');
}

fn failover_gate_violations_for_runs(runs: &[RunOutput]) -> Vec<String> {
    let mut by_group: BTreeMap<(String, u32, i32, i16), Vec<&RunOutput>> = BTreeMap::new();
    for r in runs.iter().filter(|r| r.scenario.failover.is_some()) {
        by_group
            .entry((
                r.scenario.name.clone(),
                r.topology.broker_count,
                r.topology.partitions,
                r.topology.replication_factor,
            ))
            .or_default()
            .push(r);
    }

    let mut violations = Vec::new();
    if by_group.is_empty() {
        violations.push("missing failover results".into());
        return violations;
    }

    for ((scenario, brokers, partitions, rf), group) in by_group {
        let crabka: Vec<&RunOutput> = group
            .iter()
            .copied()
            .filter(|r| r.stack == Stack::Crabka)
            .collect();
        let kafka: Vec<&RunOutput> = group
            .iter()
            .copied()
            .filter(|r| r.stack == Stack::Kafka)
            .collect();
        let label = format!("{scenario} @ {brokers} broker(s), {partitions} partitions, RF={rf}");

        let Some(c_recovery) = mean_failover_recovery(&crabka) else {
            violations.push(format!(
                "{label}: missing Crabka failover disturbance result"
            ));
            continue;
        };
        let Some(k_recovery) = mean_failover_recovery(&kafka) else {
            violations.push(format!(
                "{label}: missing kafka failover disturbance result"
            ));
            continue;
        };
        if c_recovery > k_recovery {
            violations.push(format!(
                "{label}: Crabka recovery {} is slower than kafka {}",
                c_recovery.human(),
                k_recovery.human()
            ));
        }
        compare_failover_smoothness(&mut violations, &label, &crabka, &kafka);

        require_rate_samples(&mut violations, &label, "crabka", &crabka);
        require_rate_samples(&mut violations, &label, "kafka", &kafka);
        compare_rate_recovery(
            &mut violations,
            &label,
            &crabka,
            &kafka,
            "producer",
            producer_sample_rate,
        );
        compare_rate_recovery(
            &mut violations,
            &label,
            &crabka,
            &kafka,
            "consumer",
            consumer_sample_rate,
        );
    }

    violations
}

fn compare_failover_smoothness(
    violations: &mut Vec<String>,
    label: &str,
    crabka: &[&RunOutput],
    kafka: &[&RunOutput],
) {
    if let (Some(c_drops), Some(k_drops)) =
        (mean_failover_dropped(crabka), mean_failover_dropped(kafka))
        && c_drops > k_drops
    {
        violations.push(format!(
            "{label}: Crabka dropped {c_drops:.0} messages vs kafka {k_drops:.0}"
        ));
    }
    if let (Some(c_spike), Some(k_spike)) = (
        mean_failover_latency_spike(crabka),
        mean_failover_latency_spike(kafka),
    ) && c_spike > k_spike
    {
        violations.push(format!(
            "{label}: Crabka latency spike {} is higher than kafka {}",
            c_spike.human(),
            k_spike.human()
        ));
    }
}

fn compare_rate_recovery(
    violations: &mut Vec<String>,
    label: &str,
    crabka: &[&RunOutput],
    kafka: &[&RunOutput],
    metric: &str,
    select: SampleRate,
) {
    let (Some(c_rate), Some(k_rate)) = (
        mean_rate_recovery(crabka, select),
        mean_rate_recovery(kafka, select),
    ) else {
        return;
    };

    match (c_rate.recovery, k_rate.recovery) {
        (Some(c), Some(k)) if c > k => violations.push(format!(
            "{label}: Crabka {metric} rate recovery {} is slower than kafka {}",
            c.human(),
            k.human()
        )),
        (None, Some(k)) => violations.push(format!(
            "{label}: Crabka {metric} rate did not recover while kafka recovered in {}",
            k.human()
        )),
        (None, None) => violations.push(format!("{label}: Crabka {metric} rate did not recover")),
        _ => {}
    }
}

fn require_rate_samples(
    violations: &mut Vec<String>,
    label: &str,
    stack_label: &str,
    runs: &[&RunOutput],
) {
    if runs.is_empty() {
        return;
    }
    for (idx, run) in runs.iter().enumerate() {
        if rate_recovery_for_run(run, producer_sample_rate).is_none()
            || rate_recovery_for_run(run, consumer_sample_rate).is_none()
        {
            violations.push(format!(
                "{label}: {stack_label} failover run is missing message-rate samples (run {})",
                idx + 1
            ));
        }
    }
}

fn mean_failover_recovery(runs: &[&RunOutput]) -> Option<Time> {
    let vals: Vec<f64> = runs
        .iter()
        .filter_map(|r| r.disturbance.as_ref())
        .map(|d| d.recovery_at_ms.since(d.kill_at_ms).secs_f64())
        .collect();
    (!vals.is_empty()).then(|| Time::from_secs_f64(mean(&vals)))
}

fn mean_failover_dropped(runs: &[&RunOutput]) -> Option<f64> {
    let vals: Vec<f64> = runs
        .iter()
        .filter_map(|r| r.disturbance.as_ref())
        .map(|d| to_f64(d.dropped.0))
        .collect();
    (!vals.is_empty()).then(|| mean(&vals))
}

fn mean_failover_latency_spike(runs: &[&RunOutput]) -> Option<Time> {
    let vals: Vec<f64> = runs
        .iter()
        .filter_map(|r| r.disturbance.as_ref())
        .map(|d| d.latency_spike_max.secs_f64())
        .collect();
    (!vals.is_empty()).then(|| Time::from_secs_f64(mean(&vals)))
}

fn mean_rate_recovery(runs: &[&RunOutput], select: SampleRate) -> Option<RateRecovery> {
    let vals: Vec<RateRecovery> = runs
        .iter()
        .filter_map(|r| rate_recovery_for_run(r, select))
        .collect();
    if vals.is_empty() {
        return None;
    }
    let baselines: Vec<f64> = vals.iter().map(|v| v.baseline.per_sec_f64()).collect();
    let mins: Vec<f64> = vals
        .iter()
        .map(|v| v.min_after_kill.per_sec_f64())
        .collect();
    let recoveries: Vec<f64> = vals
        .iter()
        .filter_map(|v| v.recovery.map(TimeExt::secs_f64))
        .collect();
    Some(RateRecovery {
        baseline: Frequency::from_per_sec(mean(&baselines)),
        min_after_kill: Frequency::from_per_sec(mean(&mins)),
        recovery: (!recoveries.is_empty()).then(|| Time::from_secs_f64(mean(&recoveries))),
    })
}

fn rate_recovery_for_run(r: &RunOutput, select: SampleRate) -> Option<RateRecovery> {
    let failover = r.scenario.failover.as_ref()?;
    if r.samples.is_empty() {
        return None;
    }
    // Sample offsets are measured from the *measurement* window start, so the
    // kill offset drops the warmup the scenario spent before it.
    let kill_offset = failover.kill_after - r.scenario.warmup;
    let rate_at = |s: &crate::scenario::Sample| select(s).per_sec_f64();
    let baseline: Vec<f64> = r
        .samples
        .iter()
        .filter(|s| s.t_offset_ms.as_time() < kill_offset)
        .map(rate_at)
        .collect();
    if baseline.is_empty() {
        return None;
    }
    let baseline_rate = mean(&baseline);
    let after: Vec<&crate::scenario::Sample> = r
        .samples
        .iter()
        .filter(|s| s.t_offset_ms.as_time() >= kill_offset)
        .collect();
    if after.is_empty() {
        return None;
    }
    let min_after_kill = after
        .iter()
        .map(|s| rate_at(s))
        .fold(f64::INFINITY, f64::min);
    let threshold = baseline_rate * 0.90;
    let recovery = after
        .iter()
        .find(|s| rate_at(s) >= threshold)
        .map(|s| s.t_offset_ms.as_time() - kill_offset);

    Some(RateRecovery {
        baseline: Frequency::from_per_sec(baseline_rate),
        min_after_kill: Frequency::from_per_sec(min_after_kill),
        recovery,
    })
}

/// How one of the two client message rates is read out of a time-series sample.
type SampleRate = fn(&crate::scenario::Sample) -> Frequency;

fn producer_sample_rate(s: &crate::scenario::Sample) -> Frequency {
    s.producer_rate
}

fn consumer_sample_rate(s: &crate::scenario::Sample) -> Frequency {
    s.consumer_rate
}

fn render_rate_recovery_row(
    out: &mut String,
    stack: &str,
    failover_recovery: Time,
    producer_rate: Option<RateRecovery>,
    consumer_rate: Option<RateRecovery>,
) {
    let recovery = |rate: &RateRecovery| {
        rate.recovery
            .map_or_else(|| "unrecovered".into(), |t| t.human().to_string())
    };
    match (producer_rate, consumer_rate) {
        (Some(producer_rate), Some(consumer_rate)) => push_fmt(
            out,
            format_args!(
                "| {stack} | {} | {} | {} | {} | {} | {} | {} |\n",
                failover_recovery.human(),
                producer_rate.baseline.human(),
                producer_rate.min_after_kill.human(),
                recovery(&producer_rate),
                consumer_rate.baseline.human(),
                consumer_rate.min_after_kill.human(),
                recovery(&consumer_rate),
            ),
        ),
        _ => push_fmt(
            out,
            format_args!(
                "| {stack} | {} | n/a | n/a | n/a | n/a | n/a | n/a |\n",
                failover_recovery.human()
            ),
        ),
    }
}

/// Render one comparison row: the mean (± CV) of `value` over each stack's
/// runs, plus the crabka-vs-kafka ratio of the two means.
///
/// The ratio divides two means in the same unit, so it is dimensionless
/// whatever `render` writes the cells as.
fn row_metric(
    out: &mut String,
    label: &str,
    (crabka, kafka): (&[&RunOutput], &[&RunOutput]),
    (value, render): (impl Fn(&RunOutput) -> f64, Render),
    higher_is_better: bool,
) {
    let cvals: Vec<f64> = crabka.iter().map(|&r| value(r)).collect();
    let kvals: Vec<f64> = kafka.iter().map(|&r| value(r)).collect();
    let c = mean(&cvals);
    let k = mean(&kvals);
    let ratio = if higher_is_better {
        if k > 0.0 {
            format!("{:.2}×", c / k)
        } else {
            "—".into()
        }
    } else if c > 0.0 {
        format!("{:.2}×", k / c)
    } else {
        "—".into()
    };
    push_fmt(
        out,
        format_args!(
            "| {label} | {} | {} | {ratio} |\n",
            render.cell(&cvals),
            render.cell(&kvals)
        ),
    );
}

// ── CSV exports (graph-ready) ────────────────────────────────────────────────

/// Read + parse every `*.json` in `input_dir` into `RunOutput`s, keeping the
/// source path (the per-run `-runNN` tag lives in the filename).
fn collect_runs(input_dir: &Path, strict: bool) -> Result<Vec<(PathBuf, RunOutput)>> {
    let mut runs: Vec<(PathBuf, RunOutput)> = Vec::new();
    let entries = std::fs::read_dir(input_dir)
        .with_context(|| format!("read_dir {}", input_dir.display()))?;
    for e in entries {
        let e = e.context("dir entry")?;
        let path = e.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let body =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        match serde_json::from_str::<RunOutput>(&body) {
            Ok(r) => runs.push((path, r)),
            Err(e) => {
                if strict {
                    return Err(anyhow::anyhow!("failed to parse {}: {e}", path.display()));
                }
                eprintln!("warn: skipping unparseable {}: {e}", path.display());
            }
        }
    }
    Ok(runs)
}

fn stack_str(s: Stack) -> &'static str {
    match s {
        Stack::Crabka => "crabka",
        Stack::Kafka => "kafka",
    }
}

/// The per-run tag from a result filename (`...-run07.json` → `run07`), or
/// `single` for an untagged one-off run.
fn run_tag_from_path(path: &Path) -> String {
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    if let Some(idx) = stem.rfind("-run") {
        let tag = &stem[idx + 1..];
        if tag.len() > 3 && tag["run".len()..].chars().all(|c| c.is_ascii_digit()) {
            return tag.to_string();
        }
    }
    "single".to_string()
}

/// Minimal CSV quoting: wrap in double-quotes (doubling internal quotes) only
/// when the field contains a comma, quote, or newline.
fn csv_field(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// One row per run (wide): every aggregate metric is a column. Group by
/// `(scenario, stack, broker_count)` in any tool to draw crabka-vs-kafka bars
/// with run-to-run error bars.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub fn render_csv(input_dir: &Path, strict: bool) -> Result<String> {
    let mut runs = collect_runs(input_dir, strict)?;
    runs.sort_by(|(pa, a), (pb, b)| {
        (
            a.scenario.name.as_str(),
            a.topology.broker_count,
            stack_str(a.stack),
            run_tag_from_path(pa),
        )
            .cmp(&(
                b.scenario.name.as_str(),
                b.topology.broker_count,
                stack_str(b.stack),
                run_tag_from_path(pb),
            ))
    });

    // Every column names the unit it is written in, because a CSV cell is a
    // bare number: sizes in bytes or MiB, extents in ms.
    let mut out = String::new();
    out.push_str(
        "scenario,stack,run_tag,broker_count,partitions,replication_factor,producers,consumers,\
msg_size_bytes,acks,mode_tag,duration_s,wallclock_start_unix_ms,\
msgs_produced,msgs_consumed,mb_in,mb_out,producer_msgs_per_sec,consumer_msgs_per_sec,\
producer_p50_ms,producer_p95_ms,producer_p99_ms,producer_p999_ms,producer_max_ms,\
consumer_p50_ms,consumer_p95_ms,consumer_p99_ms,consumer_p999_ms,consumer_max_ms,\
broker_cpu_seconds,mem_working_set_bytes,msgs_per_cpu_core,\
jvm_heap_used_bytes,jvm_nonheap_used_bytes,kafka_page_cache_approx_bytes,\
startup_ms,first_ack_ms,failover_recovery_ms,failover_dropped,failover_latency_spike_max_ms,\
notes,errors_count\n",
    );
    for (path, r) in &runs {
        let acks = format!("{:?}", r.scenario.acks).to_lowercase();
        let mode_tag = format!("{:?}", r.scenario.mode_tag).to_lowercase();
        let (rec, drop, spike) = match &r.disturbance {
            Some(d) => (
                d.recovery_at_ms
                    .since(d.kill_at_ms)
                    .millis_i64()
                    .to_string(),
                d.dropped.to_string(),
                format!("{:.3}", millis_f64(d.latency_spike_max)),
            ),
            None => (String::new(), String::new(), String::new()),
        };
        let opt_bytes =
            |o: Option<ByteSize>| o.map(|v| v.bytes_i64().to_string()).unwrap_or_default();
        let opt_millis =
            |o: Option<Time>| o.map(|v| v.millis_i64().to_string()).unwrap_or_default();
        let p = &r.producer_latency;
        let c = &r.consumer_e2e_latency;
        let cols = [
            csv_field(&r.scenario.name),
            stack_str(r.stack).to_string(),
            run_tag_from_path(path),
            r.topology.broker_count.to_string(),
            r.topology.partitions.to_string(),
            r.topology.replication_factor.to_string(),
            r.scenario.producers.to_string(),
            r.scenario.consumers.to_string(),
            r.scenario.msg_size.bytes_u64().to_string(),
            acks,
            mode_tag,
            r.scenario.duration.secs_i64().to_string(),
            r.wallclock_start_unix_ms.to_string(),
            r.throughput.msgs_produced.to_string(),
            r.throughput.msgs_consumed.to_string(),
            format!("{:.6}", mebibytes_f64(r.throughput.bytes_in)),
            format!("{:.6}", mebibytes_f64(r.throughput.bytes_out)),
            format!("{:.3}", r.throughput.producer_rate.per_sec_f64()),
            format!("{:.3}", r.throughput.consumer_rate.per_sec_f64()),
            format!("{:.3}", millis_f64(p.p50)),
            format!("{:.3}", millis_f64(p.p95)),
            format!("{:.3}", millis_f64(p.p99)),
            format!("{:.3}", millis_f64(p.p999)),
            format!("{:.3}", millis_f64(p.max)),
            format!("{:.3}", millis_f64(c.p50)),
            format!("{:.3}", millis_f64(c.p95)),
            format!("{:.3}", millis_f64(c.p99)),
            format!("{:.3}", millis_f64(c.p999)),
            format!("{:.3}", millis_f64(c.max)),
            format!("{:.3}", r.resource.broker_cpu.secs_f64()),
            r.resource.mem_cgroup_working_set.bytes_u64().to_string(),
            format!("{:.3}", r.resource.msgs_per_cpu_second.per_sec_f64()),
            opt_bytes(r.resource.jvm_heap_used),
            opt_bytes(r.resource.jvm_nonheap_used),
            opt_bytes(r.resource.kafka_page_cache_approx),
            opt_millis(r.startup),
            r.first_ack.millis_i64().to_string(),
            rec,
            drop,
            spike,
            csv_field(&r.notes.join("; ")),
            r.errors.len().to_string(),
        ];
        out.push_str(&cols.join(","));
        out.push('\n');
    }
    Ok(out)
}

/// Long/tidy time-series CSV: one row per (run × time-offset × metric). This is
/// the graph-ready export for plotting values *over the test* — filter by
/// `metric` and group by `(scenario, stack, run_tag)` to draw lines.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub fn render_timeseries_csv(input_dir: &Path, strict: bool) -> Result<String> {
    let mut runs = collect_runs(input_dir, strict)?;
    runs.sort_by(|(pa, a), (pb, b)| {
        (
            a.scenario.name.as_str(),
            a.topology.broker_count,
            stack_str(a.stack),
            run_tag_from_path(pa),
        )
            .cmp(&(
                b.scenario.name.as_str(),
                b.topology.broker_count,
                stack_str(b.stack),
                run_tag_from_path(pb),
            ))
    });

    let mut out = String::new();
    out.push_str("scenario,stack,broker_count,partitions,replication_factor,run_tag,t_offset_ms,metric,value\n");
    for (path, r) in &runs {
        let prefix = format!(
            "{},{},{},{},{},{}",
            csv_field(&r.scenario.name),
            stack_str(r.stack),
            r.topology.broker_count,
            r.topology.partitions,
            r.topology.replication_factor,
            run_tag_from_path(path),
        );
        for s in &r.samples {
            for (metric, value) in [
                ("producer_msgs_per_sec", s.producer_rate.per_sec_f64()),
                ("consumer_msgs_per_sec", s.consumer_rate.per_sec_f64()),
                ("producer_p50_ms", millis_f64(s.producer_p50)),
                ("producer_p99_ms", millis_f64(s.producer_p99)),
                ("consumer_e2e_p99_ms", millis_f64(s.consumer_e2e_p99)),
            ] {
                push_fmt(
                    &mut out,
                    format_args!("{prefix},{},{metric},{value:.3}\n", s.t_offset_ms),
                );
            }
        }
        for b in &r.broker_samples {
            push_fmt(
                &mut out,
                format_args!(
                    "{prefix},{},broker_cpu_cores,{:.4}\n",
                    b.t_offset_ms, b.cpu_cores
                ),
            );
            push_fmt(
                &mut out,
                format_args!(
                    "{prefix},{},broker_mem_working_set_bytes,{}\n",
                    b.t_offset_ms,
                    b.mem_working_set.bytes_u64()
                ),
            );
        }
    }
    Ok(out)
}

/// Render a self-contained Plotly HTML report (bar charts + averaged
/// time-series) from every run in `input_dir`. Delegates the aggregation +
/// figure building to [`crate::aggregate`] / [`crate::graph`].
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub fn render_html(input_dir: &Path, strict: bool, title: &str) -> Result<String> {
    let outputs: Vec<RunOutput> = collect_runs(input_dir, strict)?
        .into_iter()
        .map(|(_, r)| r)
        .collect();
    Ok(crate::graph::render_html(&outputs, title))
}

/// Render the website HTML fragment (per-run + averaged throughput/CPU/memory
/// charts) from every run in `input_dir`, pairing each run with its `runNN`
/// tag (parsed from the result filename) so per-run traces are labelled.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
pub fn render_web_fragment(input_dir: &Path, strict: bool) -> Result<String> {
    let tagged: Vec<(String, RunOutput)> = collect_runs(input_dir, strict)?
        .into_iter()
        .map(|(p, r)| (run_tag_from_path(&p), r))
        .collect();
    Ok(crate::graph::render_web_fragment(&tagged))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        ids::{MessageCount, TimeOffsetMs, WallclockMs},
        numeric::{event_rate, nonnegative_i64_to_u64},
        scenario::{
            Acks, Compression, Disturbance, LoadMode, ModeTag, Sample, Scenario, Throughput,
            Topology,
        },
    };

    fn fake_run(stack: Stack, msgs: u64) -> RunOutput {
        RunOutput {
            scenario: Scenario {
                name: "small-msg-saturate".into(),
                mode_tag: ModeTag::Ci,
                msg_size: bytes(100),
                key_size: ByteSize::ZERO,
                partitions: 6,
                replication_factor: 1,
                producers: 1,
                consumers: 1,
                mode: LoadMode::Saturate,
                acks: Acks::Leader,
                compression: Compression::None,
                linger: millis(5),
                batch_size: kibibytes(16),
                duration: secs(60),
                warmup: secs(10),
                failover: None,
            },
            stack,
            topology: Topology {
                partitions: 6,
                replication_factor: 1,
                broker_count: 1,
            },
            wallclock_start_unix_ms: WallclockMs(0),
            wallclock_end_unix_ms: WallclockMs(60_000),
            throughput: Throughput {
                msgs_produced: MessageCount(msgs),
                msgs_consumed: MessageCount(msgs),
                bytes_in: mebibytes(5),
                bytes_out: mebibytes(5),
                producer_rate: event_rate(msgs, secs(60)),
                consumer_rate: event_rate(msgs, secs(60)),
            },
            ..RunOutput::default_placeholder()
        }
    }

    fn fake_failover_run(stack: Stack, recovery: Time, post_kill_min: Frequency) -> RunOutput {
        let mut r = fake_run(stack, 600_000);
        r.scenario.name = "failover".into();
        r.scenario.mode_tag = ModeTag::Cluster;
        r.scenario.partitions = 12;
        r.scenario.replication_factor = 3;
        r.scenario.duration = secs(12);
        r.scenario.warmup = Time::ZERO;
        r.scenario.failover = Some(crate::scenario::FailoverSpec {
            kill_after: secs(4),
            target: "partition0_leader".into(),
        });
        r.topology.partitions = 12;
        r.topology.replication_factor = 3;
        r.topology.broker_count = 3;
        r.disturbance = Some(Disturbance {
            kill_at_ms: TimeOffsetMs(4_000),
            recovery_at_ms: TimeOffsetMs(4_000 + nonnegative_i64_to_u64(recovery.millis_i64())),
            dropped: MessageCount(0),
            latency_spike_max: millis(42),
        });
        r.samples = vec![
            Sample {
                t_offset_ms: TimeOffsetMs(0),
                producer_rate: per_sec(10_000),
                consumer_rate: per_sec(9_800),
                ..Sample::default()
            },
            Sample {
                t_offset_ms: TimeOffsetMs(2_000),
                producer_rate: per_sec(10_200),
                consumer_rate: per_sec(9_900),
                ..Sample::default()
            },
            Sample {
                t_offset_ms: TimeOffsetMs(4_000),
                producer_rate: post_kill_min,
                consumer_rate: post_kill_min * 0.9,
                ..Sample::default()
            },
            Sample {
                t_offset_ms: TimeOffsetMs(6_000),
                producer_rate: per_sec(9_500),
                consumer_rate: per_sec(9_200),
                ..Sample::default()
            },
        ];
        r
    }

    // Spare default impl scoped to tests.
    impl RunOutput {
        fn default_placeholder() -> Self {
            Self {
                scenario: Scenario {
                    name: "x".into(),
                    mode_tag: ModeTag::Ci,
                    msg_size: bytes(100),
                    key_size: ByteSize::ZERO,
                    partitions: 1,
                    replication_factor: 1,
                    producers: 1,
                    consumers: 1,
                    mode: LoadMode::Saturate,
                    acks: Acks::Leader,
                    compression: Compression::None,
                    linger: Time::ZERO,
                    batch_size: kibibytes(16),
                    duration: secs(1),
                    warmup: Time::ZERO,
                    failover: None,
                },
                stack: Stack::Crabka,
                topology: Topology {
                    partitions: 1,
                    replication_factor: 1,
                    broker_count: 1,
                },
                wallclock_start_unix_ms: WallclockMs(0),
                wallclock_end_unix_ms: WallclockMs(0),
                throughput: Throughput::default(),
                producer_latency: crate::scenario::LatencyPercentiles::default(),
                consumer_e2e_latency: crate::scenario::LatencyPercentiles::default(),
                resource: crate::scenario::Resource::default(),
                disturbance: None,
                startup: None,
                first_ack: Time::ZERO,
                errors: vec![],
                notes: vec![],
                samples: vec![],
                broker_samples: vec![],
            }
        }
    }

    #[test]
    fn renders_a_simple_pair() {
        let dir = tempdir().unwrap();
        let crabka = fake_run(Stack::Crabka, 600_000);
        let kafka = fake_run(Stack::Kafka, 400_000);
        std::fs::write(
            dir.path().join("crabka.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka.json"),
            serde_json::to_string(&kafka).unwrap(),
        )
        .unwrap();
        let md = render_markdown(dir.path(), true).unwrap();
        // "1.50×" is the ratio 600k / 400k.
        for needle in ["small-msg-saturate", "producer msgs/s", "1.50×"] {
            assert2::assert!(md.contains(needle));
        }
    }

    #[test]
    fn averages_multiple_runs_per_cell() {
        let dir = tempdir().unwrap();
        // Three crabka runs (600k/700k/800k → mean 700k) and three identical
        // kafka runs (400k). The report should average each stack and ratio
        // the means: 700k/400k = 1.75×.
        for (i, msgs) in [600_000u64, 700_000, 800_000].iter().enumerate() {
            std::fs::write(
                dir.path().join(format!("crabka-run{i}.json")),
                serde_json::to_string(&fake_run(Stack::Crabka, *msgs)).unwrap(),
            )
            .unwrap();
        }
        for i in 0..3 {
            std::fs::write(
                dir.path().join(format!("kafka-run{i}.json")),
                serde_json::to_string(&fake_run(Stack::Kafka, 400_000)).unwrap(),
            )
            .unwrap();
        }
        let md = render_markdown(dir.path(), true).unwrap();
        // Multi-run cells carry a coefficient-of-variation marker ("±").
        for needle in ["1.75×", "Runs averaged: crabka=3, kafka=3", "±"] {
            assert2::assert!(md.contains(needle));
        }
    }

    #[test]
    fn handles_empty_dir() {
        let dir = tempdir().unwrap();
        let md = render_markdown(dir.path(), false).unwrap();
        assert2::assert!(md.contains("no `RunOutput` JSON files found"));
    }

    #[test]
    fn failover_summary_compares_recovery_and_rate_over_time() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Crabka, secs(2), per_sec(8_000)))
                .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, secs(3), per_sec(6_000)))
                .unwrap(),
        )
        .unwrap();

        let md = render_markdown(dir.path(), true).unwrap();

        for needle in [
            "**Failover comparison:** PASS",
            "Crabka recovered 1s faster than kafka",
            "| stack | recovery ms | producer baseline msgs/s | producer min after kill msgs/s | producer rate recovery ms | consumer baseline msgs/s | consumer min after kill msgs/s | consumer rate recovery ms |",
            "| crabka | 2s | 10100/s | 8000/s | 2s | 9850/s | 7200/s | 2s |",
            "| kafka | 3s | 10100/s | 6000/s | 2s | 9850/s | 5400/s | 2s |",
        ] {
            assert2::assert!(md.contains(needle));
        }
    }

    #[test]
    fn failover_gate_passes_when_crabka_recovers_no_slower_with_rate_samples() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Crabka, secs(2), per_sec(8_000)))
                .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, secs(3), per_sec(6_000)))
                .unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(violations.is_empty());
    }

    #[test]
    fn failover_gate_fails_without_failover_results() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path()
                .join("crabka-small-msg-saturate-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_run(Stack::Crabka, 600_000)).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join("kafka-small-msg-saturate-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_run(Stack::Kafka, 400_000)).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(
            violations
                .iter()
                .any(|v| v.contains("missing failover results"))
        );
    }

    #[test]
    fn failover_gate_does_not_compare_different_topologies() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, secs(2), per_sec(8_000));
        crabka.topology.partitions = 12;
        crabka.topology.replication_factor = 3;
        let mut kafka = fake_failover_run(Stack::Kafka, secs(3), per_sec(6_000));
        kafka.topology.partitions = 24;
        kafka.topology.replication_factor = 3;
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-24p-run01.json"),
            serde_json::to_string(&kafka).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(violations.iter().any(|v| v.contains(
            "failover @ 3 broker(s), 12 partitions, RF=3: missing kafka failover disturbance result"
        )));
        assert2::assert!(violations
                .iter()
                .any(|v| v.contains("failover @ 3 broker(s), 24 partitions, RF=3: missing Crabka failover disturbance result")));
    }

    #[test]
    fn failover_gate_fails_when_crabka_recovers_slower_or_rate_samples_missing() {
        let dir = tempdir().unwrap();
        let mut kafka = fake_failover_run(Stack::Kafka, secs(2), per_sec(6_000));
        kafka.samples.clear();
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Crabka, secs(4), per_sec(8_000)))
                .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&kafka).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(
            violations
                .iter()
                .any(|v| v.contains("Crabka recovery 4s is slower than kafka 2s"))
        );
        assert2::assert!(
            violations
                .iter()
                .any(|v| v.contains("kafka failover run is missing message-rate samples"))
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_message_rate_recovers_slower_than_kafka() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, secs(2), per_sec(8_000));
        crabka.samples[3].producer_rate = per_sec(8_500);
        crabka.samples.push(Sample {
            t_offset_ms: TimeOffsetMs(8_000),
            producer_rate: per_sec(9_500),
            consumer_rate: per_sec(9_200),
            ..Sample::default()
        });
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, secs(3), per_sec(6_000)))
                .unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(
            violations.iter().any(|v| {
                v.contains("Crabka producer rate recovery 4s is slower than kafka 2s")
            })
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_message_rate_never_recovers() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, secs(2), per_sec(8_000));
        for sample in crabka
            .samples
            .iter_mut()
            .filter(|sample| sample.t_offset_ms >= 4_000)
        {
            sample.producer_rate = per_sec(8_500);
        }
        let mut kafka = fake_failover_run(Stack::Kafka, secs(3), per_sec(6_000));
        for sample in kafka
            .samples
            .iter_mut()
            .filter(|sample| sample.t_offset_ms >= 4_000)
        {
            sample.producer_rate = per_sec(8_500);
        }
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&kafka).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(
            violations
                .iter()
                .any(|v| v.contains("Crabka producer rate did not recover"))
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_consumer_rate_recovers_slower_than_kafka() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, secs(2), per_sec(8_000));
        crabka.samples[3].consumer_rate = per_sec(8_000);
        crabka.samples.push(Sample {
            t_offset_ms: TimeOffsetMs(8_000),
            producer_rate: per_sec(9_500),
            consumer_rate: per_sec(9_200),
            ..Sample::default()
        });
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, secs(3), per_sec(6_000)))
                .unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(
            violations.iter().any(|v| {
                v.contains("Crabka consumer rate recovery 4s is slower than kafka 2s")
            })
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_drops_more_messages_than_kafka() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, secs(2), per_sec(8_000));
        crabka.disturbance.as_mut().unwrap().dropped = MessageCount(5);
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, secs(3), per_sec(6_000)))
                .unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(
            violations
                .iter()
                .any(|v| v.contains("Crabka dropped 5 messages vs kafka 0"))
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_latency_spike_is_higher_than_kafka() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, secs(2), per_sec(8_000));
        crabka.disturbance.as_mut().unwrap().latency_spike_max = millis(90);
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, secs(3), per_sec(6_000)))
                .unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert2::assert!(
            violations
                .iter()
                .any(|v| v.contains("Crabka latency spike 90ms is higher than kafka 42ms"))
        );
    }

    #[test]
    fn html_report_loads_runs_and_embeds_plotly() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path()
                .join("crabka-small-msg-saturate-6broker-rf3-run01.json"),
            serde_json::to_string(&fake_run(Stack::Crabka, 600_000)).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join("kafka-small-msg-saturate-6broker-rf3-run01.json"),
            serde_json::to_string(&fake_run(Stack::Kafka, 400_000)).unwrap(),
        )
        .unwrap();
        let html = render_html(dir.path(), true, "Bench").unwrap();
        for needle in [
            "<html",
            "Bench",
            "cdn.plot.ly/plotly-3.0.1",
            "small-msg-saturate",
            "Producer throughput",
        ] {
            assert2::assert!(html.contains(needle));
        }
    }

    #[test]
    fn web_fragment_loads_runs_with_tags() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path()
                .join("crabka-small-msg-saturate-6broker-rf3-run01.json"),
            serde_json::to_string(&fake_run(Stack::Crabka, 600_000)).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join("kafka-small-msg-saturate-6broker-rf3-run01.json"),
            serde_json::to_string(&fake_run(Stack::Kafka, 400_000)).unwrap(),
        )
        .unwrap();
        let frag = render_web_fragment(dir.path(), true).unwrap();
        // A fragment, not a full page (no <html> wrapper) but loads plotly.
        for (needle, want) in [
            ("<html", false),
            ("cdn.plot.ly/plotly-3.0.1", true),
            ("Per run", true),
            ("small-msg-saturate", true),
        ] {
            assert2::assert!(frag.contains(needle) == want);
        }
    }

    #[test]
    fn summary_csv_has_header_and_one_row_per_run() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path()
                .join("crabka-small-msg-saturate-6broker-rf3-run01.json"),
            serde_json::to_string(&fake_run(Stack::Crabka, 600_000)).unwrap(),
        )
        .unwrap();
        let csv = render_csv(dir.path(), true).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert2::assert!(lines.len() == 2); // header + 1 run (index guard)
        check!(lines[0].starts_with("scenario,stack,run_tag,"));
        // run_tag parsed from the filename
        check!(lines[1].contains(",run01,"));
        check!(lines[1].starts_with("small-msg-saturate,crabka,"));
    }

    #[test]
    fn timeseries_csv_emits_long_rows_per_sample() {
        use crate::scenario::{BrokerSample, Sample};
        let dir = tempdir().unwrap();
        let mut r = fake_run(Stack::Crabka, 600_000);
        r.samples = vec![
            Sample {
                t_offset_ms: TimeOffsetMs(0),
                producer_rate: per_sec(1000),
                consumer_rate: per_sec(900),
                producer_p50: micros(1500),
                producer_p99: micros(4200),
                consumer_e2e_p99: millis(7),
            },
            Sample {
                t_offset_ms: TimeOffsetMs(2000),
                producer_rate: per_sec(1100),
                consumer_rate: per_sec(950),
                producer_p50: micros(1600),
                producer_p99: micros(4500),
                consumer_e2e_p99: micros(7500),
            },
        ];
        r.broker_samples = vec![BrokerSample {
            t_offset_ms: TimeOffsetMs(0),
            cpu_cores: 2.5,
            mem_working_set: mebibytes(1),
        }];
        std::fs::write(
            dir.path().join("crabka-x-6broker-rf3-run03.json"),
            serde_json::to_string(&r).unwrap(),
        )
        .unwrap();
        let csv = render_timeseries_csv(dir.path(), true).unwrap();
        assert2::assert!(
            csv.lines()
                .next()
                .unwrap()
                .ends_with("t_offset_ms,metric,value")
        );
        // 2 samples × 5 client metrics + 1 broker sample × 2 metrics = 12 rows.
        assert2::assert!(csv.lines().count() == 1 + 12);
        for needle in [
            ",run03,0,producer_msgs_per_sec,1000.000",
            ",run03,0,broker_cpu_cores,2.5000",
            ",run03,2000,producer_p99_ms,4.500",
        ] {
            assert2::assert!(csv.contains(needle));
        }
    }
}
