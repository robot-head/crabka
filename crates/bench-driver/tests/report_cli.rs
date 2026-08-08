use std::{path::Path, process::Command};

use assert2::check;
use crabka_bench_driver::{
    ids::{MessageCount, TimeOffsetMs, WallclockMs},
    scenario::{
        Acks, Compression, LatencyPercentiles, LoadMode, ModeTag, Resource, RunOutput, Sample,
        Scenario, Stack, Throughput, Topology,
    },
};
use crabka_units::prelude::*;

#[test]
fn failover_gate_help_names_required_evidence() {
    let output = Command::new(env!("CARGO_BIN_EXE_crabka-bench-report"))
        .arg("--help")
        .output()
        .expect("run crabka-bench-report --help");

    check!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("help output is utf8");

    check!(stdout.contains("rate, drop, latency-spike, and topology evidence"));
}

#[test]
fn failover_gate_exits_nonzero_without_failover_evidence() {
    let dir = tempfile::tempdir().expect("temp dir");
    let out = dir.path().join("SUMMARY.md");

    let output = Command::new(env!("CARGO_BIN_EXE_crabka-bench-report"))
        .arg("--input-dir")
        .arg(dir.path())
        .arg("--out")
        .arg(&out)
        .arg("--failover-gate")
        .output()
        .expect("run crabka-bench-report --failover-gate");

    check!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    check!(stderr.contains("failover gate: missing failover results"));
}

/// The `RunOutput` the driver writes must survive a round trip through the
/// report binary. The aggregator reads the same JSON encoding that the driver
/// writes, renders the dimensioned values in the operator form, and writes back
/// the unit-named CSV columns.
#[test]
fn report_reads_the_run_output_encoding_and_renders_operator_units() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_run(dir.path(), "crabka-cell-run01.json", Stack::Crabka);
    write_run(dir.path(), "kafka-cell-run01.json", Stack::Kafka);

    let summary = dir.path().join("SUMMARY.md");
    let csv = dir.path().join("summary.csv");
    let output = Command::new(env!("CARGO_BIN_EXE_crabka-bench-report"))
        .arg("--input-dir")
        .arg(dir.path())
        .arg("--out")
        .arg(&summary)
        .arg("--csv")
        .arg(&csv)
        .arg("--strict")
        .output()
        .expect("run crabka-bench-report");

    check!(output.status.success());

    let markdown = std::fs::read_to_string(&summary).expect("summary written");
    // Latencies, sizes and rates all render with their unit attached.
    for needle in [
        "Duration=1m, warmup=10s",
        "| p99 producer ack (lower better) | 4.25ms |",
        "| cgroup working set (lower better) | 300MiB |",
        "| producer msgs/s (higher better) | 10000/s |",
        "| producer byte rate (higher better) | 40MiB/s |",
    ] {
        check!(markdown.contains(needle), "missing {needle:?}");
    }

    let summary_csv = std::fs::read_to_string(&csv).expect("csv written");
    let header = summary_csv.lines().next().expect("header row");
    check!(header.contains("msg_size_bytes"));
    check!(header.contains("producer_p99_ms"));
    // Sizes in bytes, latencies in fractional milliseconds.
    check!(summary_csv.contains(",1024,leader,"));
    check!(summary_csv.contains(",4.250,"));
}

fn write_run(dir: &Path, name: &str, stack: Stack) {
    let run = RunOutput {
        scenario: Scenario {
            name: "cell".into(),
            mode_tag: ModeTag::Ci,
            msg_size: kibibytes(1),
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
            broker_count: 3,
        },
        wallclock_start_unix_ms: WallclockMs(1_700_000_000_000),
        wallclock_end_unix_ms: WallclockMs(1_700_000_060_000),
        throughput: Throughput {
            msgs_produced: MessageCount(600_000),
            msgs_consumed: MessageCount(600_000),
            bytes_in: mebibytes(2400),
            bytes_out: mebibytes(2400),
            producer_rate: per_sec(10_000),
            consumer_rate: per_sec(10_000),
        },
        producer_latency: LatencyPercentiles {
            p50: micros(1500),
            p95: micros(3200),
            p99: micros(4250),
            p999: millis(9),
            max: millis(42),
            mean: micros(1800),
            count: 600_000,
        },
        consumer_e2e_latency: LatencyPercentiles::default(),
        resource: Resource {
            broker_cpu: secs(120),
            mem_cgroup_working_set: mebibytes(300),
            msgs_per_cpu_second: per_sec(5_000),
            ..Resource::default()
        },
        disturbance: None,
        startup: Some(secs(3)),
        first_ack: millis(42),
        errors: vec![],
        notes: vec![],
        samples: vec![Sample {
            t_offset_ms: TimeOffsetMs(0),
            producer_rate: per_sec(10_000),
            consumer_rate: per_sec(10_000),
            producer_p50: micros(1500),
            producer_p99: micros(4250),
            consumer_e2e_p99: millis(7),
        }],
        broker_samples: vec![],
    };
    let json = serde_json::to_string_pretty(&run).expect("encode run output");
    std::fs::write(dir.join(name), json).expect("write run output");
}
