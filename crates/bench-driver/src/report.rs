//! Aggregate per-run JSON outputs into a single Markdown summary.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::scenario::{RunOutput, Stack};

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
                .map(|d| d.recovery_at_ms.saturating_sub(d.kill_at_ms) as f64)
                .collect();
            let dropped: Vec<f64> = dists.iter().map(|d| d.dropped as f64).collect();
            let spike: Vec<f64> = dists.iter().map(|d| d.latency_spike_max_ms).collect();
            out.push_str(&format!(
                "**Failover ({label}, n={}):** recovery {:.0} ms, {:.0} drops, max latency spike {:.1} ms.\n\n",
                dists.len(),
                mean(&recovery),
                mean(&dropped),
                mean(&spike),
            ));
        }

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
                d.recovery_at_ms.saturating_sub(d.kill_at_ms).to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::{Acks, Compression, LoadMode, ModeTag, Scenario, Throughput, Topology};
    use assert2::assert;
    use tempfile::tempdir;

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
            wallclock_start_unix_ms: 0,
            wallclock_end_unix_ms: 60_000,
            throughput: Throughput {
                msgs_produced: msgs,
                msgs_consumed: msgs,
                mb_in: 5.0,
                mb_out: 5.0,
                producer_msgs_per_sec: msgs as f64 / 60.0,
                consumer_msgs_per_sec: msgs as f64 / 60.0,
            },
            ..RunOutput::default_placeholder()
        }
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
                wallclock_start_unix_ms: 0,
                wallclock_end_unix_ms: 0,
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
        assert!(md.contains("small-msg-saturate"));
        assert!(md.contains("producer msgs/s"));
        assert!(md.contains("1.50×")); // 600k / 400k
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
        assert!(md.contains("1.75×"));
        assert!(md.contains("Runs averaged: crabka=3, kafka=3"));
        // Multi-run cells carry a coefficient-of-variation marker.
        assert!(md.contains("±"));
    }

    #[test]
    fn handles_empty_dir() {
        let dir = tempdir().unwrap();
        let md = render_markdown(dir.path(), false).unwrap();
        assert!(md.contains("no `RunOutput` JSON files found"));
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
        assert!(lines[0].starts_with("scenario,stack,run_tag,"));
        assert!(lines.len() == 2); // header + 1 run
        // run_tag parsed from the filename
        assert!(lines[1].contains(",run01,"));
        assert!(lines[1].starts_with("small-msg-saturate,crabka,"));
    }

    #[test]
    fn timeseries_csv_emits_long_rows_per_sample() {
        use crate::scenario::{BrokerSample, Sample};
        let dir = tempdir().unwrap();
        let mut r = fake_run(Stack::Crabka, 600_000);
        r.samples = vec![
            Sample {
                t_offset_ms: 0,
                producer_msgs_per_sec: 1000.0,
                consumer_msgs_per_sec: 900.0,
                producer_p50_ms: 1.5,
                producer_p99_ms: 4.2,
                consumer_e2e_p99_ms: 7.0,
            },
            Sample {
                t_offset_ms: 2000,
                producer_msgs_per_sec: 1100.0,
                consumer_msgs_per_sec: 950.0,
                producer_p50_ms: 1.6,
                producer_p99_ms: 4.5,
                consumer_e2e_p99_ms: 7.5,
            },
        ];
        r.broker_samples = vec![BrokerSample {
            t_offset_ms: 0,
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
        assert!(csv.contains(",run03,0,producer_msgs_per_sec,1000.000"));
        assert!(csv.contains(",run03,0,broker_cpu_cores,2.5000"));
        assert!(csv.contains(",run03,2000,producer_p99_ms,4.500"));
    }
}
