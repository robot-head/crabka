//! Aggregate per-run JSON outputs into a single Markdown summary.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::scenario::{RunOutput, Stack};

/// Walk `input_dir` for `*.json` files, deserialize each into a
/// `RunOutput`, group by scenario name, and emit a Markdown summary.
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
        let body = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
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

    // Group by scenario name.
    let mut by_scenario: BTreeMap<String, Vec<RunOutput>> = BTreeMap::new();
    for (_p, r) in runs {
        by_scenario.entry(r.scenario.name.clone()).or_default().push(r);
    }

    let mut out = String::new();
    out.push_str("# Crabka vs Strimzi benchmark — results\n\n");
    if by_scenario.is_empty() {
        out.push_str("_no `RunOutput` JSON files found in input dir_\n");
        return Ok(out);
    }
    out.push_str("Each scenario was run once per stack. The `ratio` column is `crabka / kafka` for throughput / efficiency (higher is better for Crabka) and `kafka / crabka` for latency / resource (lower-is-better Crabka still > 1).\n\n");

    for (name, runs) in &by_scenario {
        out.push_str(&format!("## `{name}`\n\n"));

        let crabka = runs.iter().find(|r| matches!(r.stack, Stack::Crabka));
        let kafka = runs.iter().find(|r| matches!(r.stack, Stack::Kafka));

        if let Some(r) = runs.first() {
            out.push_str(&format!(
                "Topology: partitions={}, RF={}, broker_count={} (per stack). Duration={}s, warmup={}s.\n\n",
                r.topology.partitions,
                r.topology.replication_factor,
                r.topology.broker_count,
                r.scenario.duration_s,
                r.scenario.warmup_s,
            ));
        }

        // ── Topline table ───────────────────────────────────────────────────
        out.push_str("| metric | crabka | kafka | ratio |\n");
        out.push_str("|---|---|---|---|\n");
        row_throughput(&mut out, "producer msgs/s (higher better)", crabka, kafka, |t| {
            t.throughput.producer_msgs_per_sec
        }, true);
        row_throughput(&mut out, "consumer msgs/s (higher better)", crabka, kafka, |t| {
            t.throughput.consumer_msgs_per_sec
        }, true);
        row_throughput(&mut out, "producer MB/s (higher better)", crabka, kafka, |t| {
            t.throughput.mb_in / (t.scenario.duration_s.max(1) as f64)
        }, true);
        row_throughput(&mut out, "p99 producer ack ms (lower better)", crabka, kafka, |t| {
            t.producer_latency_ms.p99_ms
        }, false);
        row_throughput(&mut out, "p99 consumer e2e ms (lower better)", crabka, kafka, |t| {
            t.consumer_e2e_latency_ms.p99_ms
        }, false);
        row_throughput(&mut out, "msgs/s per CPU-core (higher better)", crabka, kafka, |t| {
            t.resource.msgs_per_cpu_core
        }, true);
        row_throughput(&mut out, "cgroup working-set MB (lower better)", crabka, kafka, |t| {
            t.resource.mem_cgroup_working_set_bytes as f64 / 1_048_576.0
        }, false);
        row_throughput(&mut out, "startup ms (CR-apply → Ready) (lower better)", crabka, kafka, |t| {
            t.startup_ms.unwrap_or(0) as f64
        }, false);
        row_throughput(&mut out, "first-ack ms (Ready → first ack) (lower better)", crabka, kafka, |t| {
            t.first_ack_ms as f64
        }, false);
        out.push('\n');

        // ── Latency percentiles ────────────────────────────────────────────
        out.push_str("**Producer ack latency (ms):**\n\n");
        out.push_str("| percentile | crabka | kafka |\n|---|---|---|\n");
        for (label, sel) in latency_percentiles_pairs() {
            let c = crabka.map_or(0.0, |r| sel(&r.producer_latency_ms));
            let k = kafka.map_or(0.0, |r| sel(&r.producer_latency_ms));
            out.push_str(&format!("| {label} | {c:.3} | {k:.3} |\n"));
        }
        out.push('\n');

        out.push_str("**Consumer end-to-end latency (ms):**\n\n");
        out.push_str("| percentile | crabka | kafka |\n|---|---|---|\n");
        for (label, sel) in latency_percentiles_pairs() {
            let c = crabka.map_or(0.0, |r| sel(&r.consumer_e2e_latency_ms));
            let k = kafka.map_or(0.0, |r| sel(&r.consumer_e2e_latency_ms));
            out.push_str(&format!("| {label} | {c:.3} | {k:.3} |\n"));
        }
        out.push('\n');

        // ── Kafka-only memory split ─────────────────────────────────────────
        if let Some(k) = kafka
            && let (Some(heap), Some(nonheap), Some(pc)) = (
                k.resource.jvm_heap_used_bytes,
                k.resource.jvm_nonheap_used_bytes,
                k.resource.kafka_page_cache_approx_bytes,
            ) {
                out.push_str("**Kafka memory split (MiB):**\n\n");
                out.push_str(&format!(
                    "- JVM heap used: {:.1}\n- JVM non-heap used: {:.1}\n- Page-cache (approx, working-set − heap − non-heap): {:.1}\n- cgroup working-set (limit-relevant): {:.1}\n\n",
                    heap as f64 / 1_048_576.0,
                    nonheap as f64 / 1_048_576.0,
                    pc as f64 / 1_048_576.0,
                    k.resource.mem_cgroup_working_set_bytes as f64 / 1_048_576.0,
                ));
            }

        // ── Failover disturbance ────────────────────────────────────────────
        for r in runs {
            if let Some(d) = &r.disturbance {
                let stack = match r.stack {
                    Stack::Crabka => "crabka",
                    Stack::Kafka => "kafka",
                };
                let elapsed = d.recovery_at_ms.saturating_sub(d.kill_at_ms);
                out.push_str(&format!(
                    "**Failover ({stack}):** recovery in {elapsed} ms, {} drops, max latency spike {:.1} ms.\n\n",
                    d.dropped, d.latency_spike_max_ms
                ));
            }
        }

        // ── Notes & errors ──────────────────────────────────────────────────
        for r in runs {
            let stack = match r.stack {
                Stack::Crabka => "crabka",
                Stack::Kafka => "kafka",
            };
            if !r.notes.is_empty() {
                out.push_str(&format!("_{stack} notes:_ {}\n\n", r.notes.join(", ")));
            }
            if !r.errors.is_empty() {
                out.push_str(&format!(
                    "_{stack} errors:_ {}\n\n",
                    truncate_list(&r.errors, 3)
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

fn latency_percentiles_pairs(
) -> [(&'static str, fn(&crate::scenario::LatencyPercentiles) -> f64); 6] {
    [
        ("p50", |p| p.p50_ms),
        ("p95", |p| p.p95_ms),
        ("p99", |p| p.p99_ms),
        ("p99.9", |p| p.p999_ms),
        ("max", |p| p.max_ms),
        ("count", |p| p.count as f64),
    ]
}

fn row_throughput(
    out: &mut String,
    label: &str,
    crabka: Option<&RunOutput>,
    kafka: Option<&RunOutput>,
    sel: impl Fn(&RunOutput) -> f64,
    higher_is_better: bool,
) {
    let c = crabka.map_or(0.0, &sel);
    let k = kafka.map_or(0.0, &sel);
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
    out.push_str(&format!("| {label} | {c:.3} | {k:.3} | {ratio} |\n"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::{
        Acks, Compression, LoadMode, ModeTag, Scenario, Throughput, Topology,
    };
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
                producer_latency_ms: Default::default(),
                consumer_e2e_latency_ms: Default::default(),
                resource: Default::default(),
                disturbance: None,
                startup_ms: None,
                first_ack_ms: 0,
                errors: vec![],
                notes: vec![],
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
    fn handles_empty_dir() {
        let dir = tempdir().unwrap();
        let md = render_markdown(dir.path(), false).unwrap();
        assert!(md.contains("no `RunOutput` JSON files found"));
    }
}
