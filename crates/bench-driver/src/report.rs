//! Aggregate per-run JSON outputs into a single Markdown summary.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::{
    ids::TimeOffsetMs,
    scenario::{RunOutput, Stack},
};

/// Walk `input_dir` for `*.json` files, deserialize each into a
/// `RunOutput`, group by `(scenario name, broker_count)`, average each
/// metric across all runs in a group, and emit a Markdown summary.
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
        let crabka: Vec<&RunOutput> = runs
            .iter()
            .filter(|r| matches!(r.stack, Stack::Crabka))
            .collect();
        let kafka: Vec<&RunOutput> = runs
            .iter()
            .filter(|r| matches!(r.stack, Stack::Kafka))
            .collect();

        out.push_str(&format!("## `{name}` @ {brokers} broker(s)\n\n"));

        if let Some(r) = runs.first() {
            out.push_str(&format!(
                "Topology: partitions={}, RF={}, broker_count={} (per stack). Duration={}s, warmup={}s. Runs averaged: crabka={}, kafka={}.\n\n",
                r.topology.partitions,
                r.topology.replication_factor,
                r.topology.broker_count,
                r.scenario.duration_s,
                r.scenario.warmup_s,
                crabka.len(),
                kafka.len(),
            ));
        }

        // ── Topline table ───────────────────────────────────────────────────
        out.push_str("| metric | crabka | kafka | ratio |\n");
        out.push_str("|---|---|---|---|\n");
        row_metric(
            &mut out,
            "producer msgs/s (higher better)",
            &crabka,
            &kafka,
            |t| t.throughput.producer_msgs_per_sec,
            true,
        );
        row_metric(
            &mut out,
            "consumer msgs/s (higher better)",
            &crabka,
            &kafka,
            |t| t.throughput.consumer_msgs_per_sec,
            true,
        );
        row_metric(
            &mut out,
            "producer MB/s (higher better)",
            &crabka,
            &kafka,
            |t| t.throughput.mb_in / (t.scenario.duration_s.max(1) as f64),
            true,
        );
        row_metric(
            &mut out,
            "p99 producer ack ms (lower better)",
            &crabka,
            &kafka,
            |t| t.producer_latency_ms.p99_ms,
            false,
        );
        row_metric(
            &mut out,
            "p99 consumer e2e ms (lower better)",
            &crabka,
            &kafka,
            |t| t.consumer_e2e_latency_ms.p99_ms,
            false,
        );
        row_metric(
            &mut out,
            "msgs/s per CPU-core (higher better)",
            &crabka,
            &kafka,
            |t| t.resource.msgs_per_cpu_core,
            true,
        );
        row_metric(
            &mut out,
            "cgroup working-set MB (lower better)",
            &crabka,
            &kafka,
            |t| t.resource.mem_cgroup_working_set_bytes as f64 / 1_048_576.0,
            false,
        );
        row_metric(
            &mut out,
            "startup ms (CR-apply → Ready) (lower better)",
            &crabka,
            &kafka,
            |t| t.startup_ms.unwrap_or(0) as f64,
            false,
        );
        row_metric(
            &mut out,
            "first-ack ms (Ready → first ack) (lower better)",
            &crabka,
            &kafka,
            |t| t.first_ack_ms as f64,
            false,
        );
        out.push('\n');

        // ── Latency percentiles (mean across runs) ──────────────────────────
        out.push_str("**Producer ack latency (ms):**\n\n");
        out.push_str("| percentile | crabka | kafka |\n|---|---|---|\n");
        for (label, sel) in latency_percentiles_pairs() {
            let c: Vec<f64> = crabka.iter().map(|r| sel(&r.producer_latency_ms)).collect();
            let k: Vec<f64> = kafka.iter().map(|r| sel(&r.producer_latency_ms)).collect();
            out.push_str(&format!(
                "| {label} | {} | {} |\n",
                fmt_cell(&c),
                fmt_cell(&k)
            ));
        }
        out.push('\n');

        out.push_str("**Consumer end-to-end latency (ms):**\n\n");
        out.push_str("| percentile | crabka | kafka |\n|---|---|---|\n");
        for (label, sel) in latency_percentiles_pairs() {
            let c: Vec<f64> = crabka
                .iter()
                .map(|r| sel(&r.consumer_e2e_latency_ms))
                .collect();
            let k: Vec<f64> = kafka
                .iter()
                .map(|r| sel(&r.consumer_e2e_latency_ms))
                .collect();
            out.push_str(&format!(
                "| {label} | {} | {} |\n",
                fmt_cell(&c),
                fmt_cell(&k)
            ));
        }
        out.push('\n');

        // ── Kafka-only memory split (mean across kafka runs that report it) ──
        let heap: Vec<f64> = kafka
            .iter()
            .filter_map(|r| r.resource.jvm_heap_used_bytes)
            .map(|x| x as f64)
            .collect();
        let nonheap: Vec<f64> = kafka
            .iter()
            .filter_map(|r| r.resource.jvm_nonheap_used_bytes)
            .map(|x| x as f64)
            .collect();
        let pc: Vec<f64> = kafka
            .iter()
            .filter_map(|r| r.resource.kafka_page_cache_approx_bytes)
            .map(|x| x as f64)
            .collect();
        if !heap.is_empty() && !nonheap.is_empty() && !pc.is_empty() {
            let ws: Vec<f64> = kafka
                .iter()
                .map(|r| r.resource.mem_cgroup_working_set_bytes as f64)
                .collect();
            out.push_str("**Kafka memory split (MiB):**\n\n");
            out.push_str(&format!(
                "- JVM heap used: {:.1}\n- JVM non-heap used: {:.1}\n- Page-cache (approx, working-set − heap − non-heap): {:.1}\n- cgroup working-set (limit-relevant): {:.1}\n\n",
                mean(&heap) / 1_048_576.0,
                mean(&nonheap) / 1_048_576.0,
                mean(&pc) / 1_048_576.0,
                mean(&ws) / 1_048_576.0,
            ));
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
                .map(|d| d.recovery_at_ms.0.saturating_sub(d.kill_at_ms.0) as f64)
                .collect();
            let dropped: Vec<f64> = dists.iter().map(|d| d.dropped.0 as f64).collect();
            let spike: Vec<f64> = dists.iter().map(|d| d.latency_spike_max_ms).collect();
            out.push_str(&format!(
                "**Failover ({label}, n={}):** recovery {:.0} ms, {:.0} drops, max latency spike {:.1} ms.\n\n",
                dists.len(),
                mean(&recovery),
                mean(&dropped),
                mean(&spike),
            ));
        }
        render_failover_comparison(&mut out, &crabka, &kafka);

        // ── Notes & errors (deduped across runs) ────────────────────────────
        for (label, stack_runs) in [("crabka", &crabka), ("kafka", &kafka)] {
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
                out.push_str(&format!("_{label} notes:_ {}\n\n", notes.join(", ")));
            }
            if !errors.is_empty() {
                out.push_str(&format!(
                    "_{label} errors:_ {}\n\n",
                    truncate_list(&errors, 3)
                ));
            }
        }
        out.push('\n');
    }

    Ok(out)
}

/// Return human-readable failover gate violations. An empty vector means every
/// failover cell with both stacks present has the evidence needed for the
/// objective: Crabka recovered no slower than Kafka, and both stacks emitted
/// producer and consumer message-rate samples over time.
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

fn latency_percentiles_pairs() -> [(&'static str, LatencySelector); 6] {
    [
        ("p50", |p| p.p50_ms),
        ("p95", |p| p.p95_ms),
        ("p99", |p| p.p99_ms),
        ("p99.9", |p| p.p999_ms),
        ("max", |p| p.max_ms),
        ("count", |p| p.count as f64),
    ]
}

/// Arithmetic mean of a sample. Empty sample → 0.0 (an absent stack renders
/// as a zero cell, matching the pre-averaging behaviour).
fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

/// Sample standard deviation (Bessel-corrected). Zero for fewer than two
/// samples — a single run has no measurable spread.
fn sample_stddev(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    let m = mean(v);
    let var = v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() as f64 - 1.0);
    var.sqrt()
}

#[derive(Debug, Clone, Copy)]
struct RateRecovery {
    baseline_mps: f64,
    min_after_kill_mps: f64,
    recovery_ms: Option<u64>,
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
    let delta = (c_recovery - k_recovery).abs().round() as u64;
    let faster = if c_recovery <= k_recovery {
        "faster than"
    } else {
        "slower than"
    };
    out.push_str(&format!(
        "**Failover comparison:** {verdict} — Crabka recovered {delta} ms {faster} kafka (crabka {c_recovery:.0} ms, kafka {k_recovery:.0} ms).\n\n",
    ));

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
                "{label}: Crabka recovery {c_recovery:.0} ms is slower than kafka {k_recovery:.0} ms"
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
            "{label}: Crabka latency spike {c_spike:.1} ms is higher than kafka {k_spike:.1} ms"
        ));
    }
}

fn compare_rate_recovery(
    violations: &mut Vec<String>,
    label: &str,
    crabka: &[&RunOutput],
    kafka: &[&RunOutput],
    metric: &str,
    select: fn(&crate::scenario::Sample) -> f64,
) {
    let (Some(c_rate), Some(k_rate)) = (
        mean_rate_recovery(crabka, select),
        mean_rate_recovery(kafka, select),
    ) else {
        return;
    };

    match (c_rate.recovery_ms, k_rate.recovery_ms) {
        (Some(c_ms), Some(k_ms)) if c_ms > k_ms => violations.push(format!(
            "{label}: Crabka {metric} rate recovery {c_ms} ms is slower than kafka {k_ms} ms"
        )),
        (None, Some(k_ms)) => violations.push(format!(
            "{label}: Crabka {metric} rate did not recover while kafka recovered in {k_ms} ms"
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

fn mean_failover_recovery(runs: &[&RunOutput]) -> Option<f64> {
    let vals: Vec<f64> = runs
        .iter()
        .filter_map(|r| r.disturbance.as_ref())
        .map(|d| d.recovery_at_ms.0.saturating_sub(d.kill_at_ms.0) as f64)
        .collect();
    (!vals.is_empty()).then(|| mean(&vals))
}

fn mean_failover_dropped(runs: &[&RunOutput]) -> Option<f64> {
    let vals: Vec<f64> = runs
        .iter()
        .filter_map(|r| r.disturbance.as_ref())
        .map(|d| d.dropped.0 as f64)
        .collect();
    (!vals.is_empty()).then(|| mean(&vals))
}

fn mean_failover_latency_spike(runs: &[&RunOutput]) -> Option<f64> {
    let vals: Vec<f64> = runs
        .iter()
        .filter_map(|r| r.disturbance.as_ref())
        .map(|d| d.latency_spike_max_ms)
        .collect();
    (!vals.is_empty()).then(|| mean(&vals))
}

fn mean_rate_recovery(
    runs: &[&RunOutput],
    select: fn(&crate::scenario::Sample) -> f64,
) -> Option<RateRecovery> {
    let vals: Vec<RateRecovery> = runs
        .iter()
        .filter_map(|r| rate_recovery_for_run(r, select))
        .collect();
    if vals.is_empty() {
        return None;
    }
    let baselines: Vec<f64> = vals.iter().map(|v| v.baseline_mps).collect();
    let mins: Vec<f64> = vals.iter().map(|v| v.min_after_kill_mps).collect();
    let recoveries: Vec<f64> = vals
        .iter()
        .filter_map(|v| v.recovery_ms.map(|ms| ms as f64))
        .collect();
    Some(RateRecovery {
        baseline_mps: mean(&baselines),
        min_after_kill_mps: mean(&mins),
        recovery_ms: (!recoveries.is_empty()).then(|| mean(&recoveries).round() as u64),
    })
}

fn rate_recovery_for_run(
    r: &RunOutput,
    select: fn(&crate::scenario::Sample) -> f64,
) -> Option<RateRecovery> {
    let failover = r.scenario.failover.as_ref()?;
    if r.samples.is_empty() {
        return None;
    }
    let kill_offset_ms = TimeOffsetMs(
        failover
            .kill_at_s
            .saturating_sub(r.scenario.warmup_s)
            .saturating_mul(1000),
    );
    let baseline: Vec<f64> = r
        .samples
        .iter()
        .filter(|s| s.t_offset_ms < kill_offset_ms)
        .map(select)
        .collect();
    if baseline.is_empty() {
        return None;
    }
    let baseline_mps = mean(&baseline);
    let after: Vec<&crate::scenario::Sample> = r
        .samples
        .iter()
        .filter(|s| s.t_offset_ms >= kill_offset_ms)
        .collect();
    if after.is_empty() {
        return None;
    }
    let min_after_kill_mps = after
        .iter()
        .map(|s| select(s))
        .fold(f64::INFINITY, f64::min);
    let threshold = baseline_mps * 0.90;
    let recovery_ms = after
        .iter()
        .find(|s| select(s) >= threshold)
        .map(|s| s.t_offset_ms.0.saturating_sub(kill_offset_ms.0));

    Some(RateRecovery {
        baseline_mps,
        min_after_kill_mps,
        recovery_ms,
    })
}

fn producer_sample_rate(s: &crate::scenario::Sample) -> f64 {
    s.producer_msgs_per_sec
}

fn consumer_sample_rate(s: &crate::scenario::Sample) -> f64 {
    s.consumer_msgs_per_sec
}

fn render_rate_recovery_row(
    out: &mut String,
    stack: &str,
    failover_recovery_ms: f64,
    producer_rate: Option<RateRecovery>,
    consumer_rate: Option<RateRecovery>,
) {
    match (producer_rate, consumer_rate) {
        (Some(producer_rate), Some(consumer_rate)) => out.push_str(&format!(
            "| {stack} | {failover_recovery_ms:.0} | {:.0} | {:.0} | {} | {:.0} | {:.0} | {} |\n",
            producer_rate.baseline_mps,
            producer_rate.min_after_kill_mps,
            producer_rate
                .recovery_ms
                .map_or_else(|| "unrecovered".into(), |ms| ms.to_string()),
            consumer_rate.baseline_mps,
            consumer_rate.min_after_kill_mps,
            consumer_rate
                .recovery_ms
                .map_or_else(|| "unrecovered".into(), |ms| ms.to_string())
        )),
        _ => out.push_str(&format!(
            "| {stack} | {failover_recovery_ms:.0} | n/a | n/a | n/a | n/a | n/a | n/a |\n"
        )),
    }
}

/// Render per-run samples as `mean` (single run) or `mean (±cv%)` (multiple
/// runs), where cv is the coefficient of variation.
fn fmt_cell(v: &[f64]) -> String {
    let m = mean(v);
    if v.len() > 1 {
        let cv = if m == 0.0 {
            0.0
        } else {
            (sample_stddev(v) / m).abs() * 100.0
        };
        format!("{m:.3} (±{cv:.0}%)")
    } else {
        format!("{m:.3}")
    }
}

/// Render one comparison row: the mean (± CV) of `sel` over each stack's
/// runs, plus the crabka-vs-kafka ratio of the two means.
fn row_metric(
    out: &mut String,
    label: &str,
    crabka: &[&RunOutput],
    kafka: &[&RunOutput],
    sel: impl Fn(&RunOutput) -> f64,
    higher_is_better: bool,
) {
    let cvals: Vec<f64> = crabka.iter().map(|&r| sel(r)).collect();
    let kvals: Vec<f64> = kafka.iter().map(|&r| sel(r)).collect();
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
    out.push_str(&format!(
        "| {label} | {} | {} | {ratio} |\n",
        fmt_cell(&cvals),
        fmt_cell(&kvals)
    ));
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
                    .0
                    .saturating_sub(d.kill_at_ms.0)
                    .to_string(),
                d.dropped.to_string(),
                format!("{:.3}", d.latency_spike_max_ms),
            ),
            None => (String::new(), String::new(), String::new()),
        };
        let opt_u = |o: Option<u64>| o.map(|v| v.to_string()).unwrap_or_default();
        let opt_i = |o: Option<i64>| o.map(|v| v.to_string()).unwrap_or_default();
        let p = &r.producer_latency_ms;
        let c = &r.consumer_e2e_latency_ms;
        let cols = [
            csv_field(&r.scenario.name),
            stack_str(r.stack).to_string(),
            run_tag_from_path(path),
            r.topology.broker_count.to_string(),
            r.topology.partitions.to_string(),
            r.topology.replication_factor.to_string(),
            r.scenario.producers.to_string(),
            r.scenario.consumers.to_string(),
            r.scenario.msg_size_bytes.to_string(),
            acks,
            mode_tag,
            r.scenario.duration_s.to_string(),
            r.wallclock_start_unix_ms.to_string(),
            r.throughput.msgs_produced.to_string(),
            r.throughput.msgs_consumed.to_string(),
            format!("{:.6}", r.throughput.mb_in),
            format!("{:.6}", r.throughput.mb_out),
            format!("{:.3}", r.throughput.producer_msgs_per_sec),
            format!("{:.3}", r.throughput.consumer_msgs_per_sec),
            format!("{:.3}", p.p50_ms),
            format!("{:.3}", p.p95_ms),
            format!("{:.3}", p.p99_ms),
            format!("{:.3}", p.p999_ms),
            format!("{:.3}", p.max_ms),
            format!("{:.3}", c.p50_ms),
            format!("{:.3}", c.p95_ms),
            format!("{:.3}", c.p99_ms),
            format!("{:.3}", c.p999_ms),
            format!("{:.3}", c.max_ms),
            format!("{:.3}", r.resource.broker_cpu_seconds),
            r.resource.mem_cgroup_working_set_bytes.to_string(),
            format!("{:.3}", r.resource.msgs_per_cpu_core),
            opt_u(r.resource.jvm_heap_used_bytes),
            opt_u(r.resource.jvm_nonheap_used_bytes),
            opt_i(r.resource.kafka_page_cache_approx_bytes),
            opt_u(r.startup_ms),
            r.first_ack_ms.to_string(),
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
                ("producer_msgs_per_sec", s.producer_msgs_per_sec),
                ("consumer_msgs_per_sec", s.consumer_msgs_per_sec),
                ("producer_p50_ms", s.producer_p50_ms),
                ("producer_p99_ms", s.producer_p99_ms),
                ("consumer_e2e_p99_ms", s.consumer_e2e_p99_ms),
            ] {
                out.push_str(&format!("{prefix},{},{metric},{value:.3}\n", s.t_offset_ms));
            }
        }
        for b in &r.broker_samples {
            out.push_str(&format!(
                "{prefix},{},broker_cpu_cores,{:.4}\n",
                b.t_offset_ms, b.cpu_cores
            ));
            out.push_str(&format!(
                "{prefix},{},broker_mem_working_set_bytes,{}\n",
                b.t_offset_ms, b.mem_working_set_bytes
            ));
        }
    }
    Ok(out)
}

/// Render a self-contained Plotly HTML report (bar charts + averaged
/// time-series) from every run in `input_dir`. Delegates the aggregation +
/// figure building to [`crate::aggregate`] / [`crate::graph`].
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
pub fn render_web_fragment(input_dir: &Path, strict: bool) -> Result<String> {
    let tagged: Vec<(String, RunOutput)> = collect_runs(input_dir, strict)?
        .into_iter()
        .map(|(p, r)| (run_tag_from_path(&p), r))
        .collect();
    Ok(crate::graph::render_web_fragment(&tagged))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        ids::{MessageCount, WallclockMs},
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
                msg_size_bytes: 100,
                key_size_bytes: 0,
                partitions: 6,
                replication_factor: 1,
                producers: 1,
                consumers: 1,
                mode: LoadMode::Saturate,
                acks: Acks::Leader,
                compression: Compression::None,
                linger_ms: 5,
                batch_size: 16384,
                duration_s: 60,
                warmup_s: 10,
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
                mb_in: 5.0,
                mb_out: 5.0,
                producer_msgs_per_sec: msgs as f64 / 60.0,
                consumer_msgs_per_sec: msgs as f64 / 60.0,
            },
            ..RunOutput::default_placeholder()
        }
    }

    fn fake_failover_run(stack: Stack, recovery_ms: u64, post_kill_min_mps: f64) -> RunOutput {
        let mut r = fake_run(stack, 600_000);
        r.scenario.name = "failover".into();
        r.scenario.mode_tag = ModeTag::Cluster;
        r.scenario.partitions = 12;
        r.scenario.replication_factor = 3;
        r.scenario.duration_s = 12;
        r.scenario.warmup_s = 0;
        r.scenario.failover = Some(crate::scenario::FailoverSpec {
            kill_at_s: 4,
            target: "partition0_leader".into(),
        });
        r.topology.partitions = 12;
        r.topology.replication_factor = 3;
        r.topology.broker_count = 3;
        r.disturbance = Some(Disturbance {
            kill_at_ms: TimeOffsetMs(4_000),
            recovery_at_ms: TimeOffsetMs(4_000 + recovery_ms),
            dropped: MessageCount(0),
            latency_spike_max_ms: 42.0,
        });
        r.samples = vec![
            Sample {
                t_offset_ms: TimeOffsetMs(0),
                producer_msgs_per_sec: 10_000.0,
                consumer_msgs_per_sec: 9_800.0,
                ..Sample::default()
            },
            Sample {
                t_offset_ms: TimeOffsetMs(2_000),
                producer_msgs_per_sec: 10_200.0,
                consumer_msgs_per_sec: 9_900.0,
                ..Sample::default()
            },
            Sample {
                t_offset_ms: TimeOffsetMs(4_000),
                producer_msgs_per_sec: post_kill_min_mps,
                consumer_msgs_per_sec: post_kill_min_mps * 0.9,
                ..Sample::default()
            },
            Sample {
                t_offset_ms: TimeOffsetMs(6_000),
                producer_msgs_per_sec: 9_500.0,
                consumer_msgs_per_sec: 9_200.0,
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
                    msg_size_bytes: 100,
                    key_size_bytes: 0,
                    partitions: 1,
                    replication_factor: 1,
                    producers: 1,
                    consumers: 1,
                    mode: LoadMode::Saturate,
                    acks: Acks::Leader,
                    compression: Compression::None,
                    linger_ms: 0,
                    batch_size: 16384,
                    duration_s: 1,
                    warmup_s: 0,
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
                producer_latency_ms: crate::scenario::LatencyPercentiles::default(),
                consumer_e2e_latency_ms: crate::scenario::LatencyPercentiles::default(),
                resource: crate::scenario::Resource::default(),
                disturbance: None,
                startup_ms: None,
                first_ack_ms: 0,
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
            assert!(md.contains(needle), "missing {needle:?} in:\n{md}");
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
            assert!(md.contains(needle), "missing {needle:?} in:\n{md}");
        }
    }

    #[test]
    fn handles_empty_dir() {
        let dir = tempdir().unwrap();
        let md = render_markdown(dir.path(), false).unwrap();
        assert!(md.contains("no `RunOutput` JSON files found"));
    }

    #[test]
    fn failover_summary_compares_recovery_and_rate_over_time() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Crabka, 2_000, 8_000.0)).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, 3_000, 6_000.0)).unwrap(),
        )
        .unwrap();

        let md = render_markdown(dir.path(), true).unwrap();

        for needle in [
            "**Failover comparison:** PASS",
            "Crabka recovered 1000 ms faster than kafka",
            "| stack | recovery ms | producer baseline msgs/s | producer min after kill msgs/s | producer rate recovery ms | consumer baseline msgs/s | consumer min after kill msgs/s | consumer rate recovery ms |",
            "| crabka | 2000 | 10100 | 8000 | 2000 | 9850 | 7200 | 2000 |",
            "| kafka | 3000 | 10100 | 6000 | 2000 | 9850 | 5400 | 2000 |",
        ] {
            assert!(md.contains(needle), "missing {needle:?} in:\n{md}");
        }
    }

    #[test]
    fn failover_gate_passes_when_crabka_recovers_no_slower_with_rate_samples() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Crabka, 2_000, 8_000.0)).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, 3_000, 6_000.0)).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert!(
            violations.is_empty(),
            "unexpected violations: {violations:?}"
        );
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

        assert!(
            violations
                .iter()
                .any(|v| v.contains("missing failover results")),
            "missing no-failover violation: {violations:?}"
        );
    }

    #[test]
    fn failover_gate_does_not_compare_different_topologies() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, 2_000, 8_000.0);
        crabka.topology.partitions = 12;
        crabka.topology.replication_factor = 3;
        let mut kafka = fake_failover_run(Stack::Kafka, 3_000, 6_000.0);
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

        assert!(
            violations
                .iter()
                .any(|v| v.contains("failover @ 3 broker(s), 12 partitions, RF=3: missing kafka failover disturbance result")),
            "missing crabka-topology violation: {violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|v| v.contains("failover @ 3 broker(s), 24 partitions, RF=3: missing Crabka failover disturbance result")),
            "missing kafka-topology violation: {violations:?}"
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_recovers_slower_or_rate_samples_missing() {
        let dir = tempdir().unwrap();
        let mut kafka = fake_failover_run(Stack::Kafka, 2_000, 6_000.0);
        kafka.samples.clear();
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Crabka, 4_000, 8_000.0)).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&kafka).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert!(
            violations
                .iter()
                .any(|v| v.contains("Crabka recovery 4000 ms is slower than kafka 2000 ms")),
            "missing slower-recovery violation: {violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|v| v.contains("kafka failover run is missing message-rate samples")),
            "missing rate-sample violation: {violations:?}"
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_message_rate_recovers_slower_than_kafka() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, 2_000, 8_000.0);
        crabka.samples[3].producer_msgs_per_sec = 8_500.0;
        crabka.samples.push(Sample {
            t_offset_ms: TimeOffsetMs(8_000),
            producer_msgs_per_sec: 9_500.0,
            consumer_msgs_per_sec: 9_200.0,
            ..Sample::default()
        });
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, 3_000, 6_000.0)).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert!(
            violations.iter().any(|v| v
                .contains("Crabka producer rate recovery 4000 ms is slower than kafka 2000 ms")),
            "missing rate-recovery violation: {violations:?}"
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_message_rate_never_recovers() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, 2_000, 8_000.0);
        for sample in crabka
            .samples
            .iter_mut()
            .filter(|sample| sample.t_offset_ms >= 4_000)
        {
            sample.producer_msgs_per_sec = 8_500.0;
        }
        let mut kafka = fake_failover_run(Stack::Kafka, 3_000, 6_000.0);
        for sample in kafka
            .samples
            .iter_mut()
            .filter(|sample| sample.t_offset_ms >= 4_000)
        {
            sample.producer_msgs_per_sec = 8_500.0;
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

        assert!(
            violations
                .iter()
                .any(|v| v.contains("Crabka producer rate did not recover")),
            "missing unrecovered-rate violation: {violations:?}"
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_consumer_rate_recovers_slower_than_kafka() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, 2_000, 8_000.0);
        crabka.samples[3].consumer_msgs_per_sec = 8_000.0;
        crabka.samples.push(Sample {
            t_offset_ms: TimeOffsetMs(8_000),
            producer_msgs_per_sec: 9_500.0,
            consumer_msgs_per_sec: 9_200.0,
            ..Sample::default()
        });
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, 3_000, 6_000.0)).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert!(
            violations.iter().any(|v| v
                .contains("Crabka consumer rate recovery 4000 ms is slower than kafka 2000 ms")),
            "missing consumer-rate violation: {violations:?}"
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_drops_more_messages_than_kafka() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, 2_000, 8_000.0);
        crabka.disturbance.as_mut().unwrap().dropped = MessageCount(5);
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, 3_000, 6_000.0)).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert!(
            violations
                .iter()
                .any(|v| v.contains("Crabka dropped 5 messages vs kafka 0")),
            "missing dropped-message violation: {violations:?}"
        );
    }

    #[test]
    fn failover_gate_fails_when_crabka_latency_spike_is_higher_than_kafka() {
        let dir = tempdir().unwrap();
        let mut crabka = fake_failover_run(Stack::Crabka, 2_000, 8_000.0);
        crabka.disturbance.as_mut().unwrap().latency_spike_max_ms = 90.0;
        std::fs::write(
            dir.path().join("crabka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&crabka).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("kafka-failover-3broker-rf3-run01.json"),
            serde_json::to_string(&fake_failover_run(Stack::Kafka, 3_000, 6_000.0)).unwrap(),
        )
        .unwrap();

        let violations = failover_gate_violations(dir.path(), true).unwrap();

        assert!(
            violations
                .iter()
                .any(|v| v.contains("Crabka latency spike 90.0 ms is higher than kafka 42.0 ms")),
            "missing latency-spike violation: {violations:?}"
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
            assert!(html.contains(needle), "missing {needle:?} in:\n{html}");
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
            assert!(frag.contains(needle) == want, "{needle:?} in:\n{frag}");
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
        assert!(lines.len() == 2); // header + 1 run (index guard)
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
                producer_msgs_per_sec: 1000.0,
                consumer_msgs_per_sec: 900.0,
                producer_p50_ms: 1.5,
                producer_p99_ms: 4.2,
                consumer_e2e_p99_ms: 7.0,
            },
            Sample {
                t_offset_ms: TimeOffsetMs(2000),
                producer_msgs_per_sec: 1100.0,
                consumer_msgs_per_sec: 950.0,
                producer_p50_ms: 1.6,
                producer_p99_ms: 4.5,
                consumer_e2e_p99_ms: 7.5,
            },
        ];
        r.broker_samples = vec![BrokerSample {
            t_offset_ms: TimeOffsetMs(0),
            cpu_cores: 2.5,
            mem_working_set_bytes: 1_048_576,
        }];
        std::fs::write(
            dir.path().join("crabka-x-6broker-rf3-run03.json"),
            serde_json::to_string(&r).unwrap(),
        )
        .unwrap();
        let csv = render_timeseries_csv(dir.path(), true).unwrap();
        assert!(
            csv.lines()
                .next()
                .unwrap()
                .ends_with("t_offset_ms,metric,value")
        );
        // 2 samples × 5 client metrics + 1 broker sample × 2 metrics = 12 rows.
        assert!(csv.lines().count() == 1 + 12);
        for needle in [
            ",run03,0,producer_msgs_per_sec,1000.000",
            ",run03,0,broker_cpu_cores,2.5000",
            ",run03,2000,producer_p99_ms,4.500",
        ] {
            assert!(csv.contains(needle), "missing {needle:?} in:\n{csv}");
        }
    }
}
