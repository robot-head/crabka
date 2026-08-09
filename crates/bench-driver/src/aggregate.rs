//! Cross-run aggregation, the "accurate averages" layer.
//!
//! A benchmark *cell* is one `(scenario name, broker_count)`. The harness runs
//! each cell `N` times per stack. This module reduces those `N` [`RunOutput`]s
//! into a mean ± sample-stddev per metric, and averages the per-interval time
//! series across runs at each time offset. Both the Markdown summary and the
//! Plotly graphs read these aggregates, so one place computes "the average" and
//! no renderer re-derives it.

use std::collections::BTreeMap;

use crabka_units::prelude::*;

use crate::{
    ids::TimeOffsetMs,
    numeric::{mebibytes_f64, millis_f64, to_f64},
    scenario::{BrokerSample, RunOutput, Sample, Stack},
};

/// Mean and spread of a single scalar metric over the runs of one
/// `(cell, stack)`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Stat {
    pub mean: f64,
    /// Bessel-corrected sample standard deviation. This is `0.0` for fewer than
    /// two runs, because a single run has no measurable spread.
    pub stddev: f64,
    pub n: usize,
}

impl Stat {
    /// Coefficient of variation as a percentage (`stddev ÷ mean × 100`). This is
    /// `0` when the mean is `0`, so an absent or zero metric renders cleanly.
    #[must_use]
    pub fn cv_percent(&self) -> f64 {
        if self.mean == 0.0 {
            0.0
        } else {
            (self.stddev / self.mean).abs() * 100.0
        }
    }
}

/// Mean, sample stddev, and count over a sample of run-level values.
#[must_use]
pub fn stat(values: &[f64]) -> Stat {
    let n = values.len();
    if n == 0 {
        return Stat::default();
    }
    let mean = values.iter().sum::<f64>() / to_f64(n);
    let stddev = if n < 2 {
        0.0
    } else {
        let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (to_f64(n) - 1.0);
        var.sqrt()
    };
    Stat { mean, stddev, n }
}

/// A headline scalar metric. It holds its key, a way to pull it from a run, and
/// which direction counts as "better", so a renderer can colour and ratio it
/// correctly.
///
/// `extract` returns a plain number in the metric's stated `unit`, because the
/// statistics below and the Plotly axes above are dimension-agnostic. A mean and
/// a stddev run over whatever unit the axis carries. The quantity-to-number seam
/// is in each metric's `extract`.
#[derive(Clone, Copy)]
pub struct ScalarMetric {
    pub key: &'static str,
    pub label: &'static str,
    pub unit: &'static str,
    pub higher_better: bool,
    extract: fn(&RunOutput) -> f64,
}

impl ScalarMetric {
    #[must_use]
    pub fn value(&self, r: &RunOutput) -> f64 {
        (self.extract)(r)
    }
}

/// The headline scalar metrics rendered as crabka-vs-kafka bars.
#[must_use]
pub fn scalar_metrics() -> Vec<ScalarMetric> {
    vec![
        ScalarMetric {
            key: "producer_msgs_per_sec",
            label: "Producer throughput",
            unit: "msgs/s",
            higher_better: true,
            extract: |r| r.throughput.producer_rate.per_sec_f64(),
        },
        ScalarMetric {
            key: "consumer_msgs_per_sec",
            label: "Consumer throughput",
            unit: "msgs/s",
            higher_better: true,
            extract: |r| r.throughput.consumer_rate.per_sec_f64(),
        },
        ScalarMetric {
            key: "producer_p99_ms",
            label: "Producer ack p99",
            unit: "ms",
            higher_better: false,
            extract: |r| millis_f64(r.producer_latency.p99),
        },
        ScalarMetric {
            key: "consumer_e2e_p99_ms",
            label: "Consumer e2e p99",
            unit: "ms",
            higher_better: false,
            extract: |r| millis_f64(r.consumer_e2e_latency.p99),
        },
        ScalarMetric {
            key: "msgs_per_cpu_core",
            label: "Efficiency",
            unit: "msgs/s per CPU-core",
            higher_better: true,
            extract: |r| r.resource.msgs_per_cpu_second.per_sec_f64(),
        },
        ScalarMetric {
            key: "mem_working_set_mb",
            label: "Broker working set",
            unit: "MiB",
            higher_better: false,
            extract: |r| mebibytes_f64(r.resource.mem_cgroup_working_set),
        },
    ]
}

/// Per-metric aggregates for one stack within a cell.
#[derive(Debug, Clone, Default)]
pub struct StackAgg {
    pub n_runs: usize,
    /// A map from metric key to the [`Stat`] across this stack's runs in the
    /// cell.
    pub metrics: BTreeMap<&'static str, Stat>,
}

/// One benchmark cell. It holds a `(scenario, topology)` with the crabka and
/// kafka aggregates side by side.
#[derive(Debug, Clone)]
pub struct CellAgg {
    pub scenario: String,
    pub partitions: i32,
    pub broker_count: u32,
    pub crabka: StackAgg,
    pub kafka: StackAgg,
}

/// Groups runs by `(scenario, broker_count)` and reduces each stack's runs to a
/// per-metric [`Stat`]. This returns the cells in `(scenario, broker_count)`
/// order.
#[must_use]
pub fn aggregate_cells(runs: &[RunOutput]) -> Vec<CellAgg> {
    let metrics = scalar_metrics();
    let mut by_cell: BTreeMap<(String, u32), Vec<&RunOutput>> = BTreeMap::new();
    for r in runs {
        by_cell
            .entry((r.scenario.name.clone(), r.topology.broker_count))
            .or_default()
            .push(r);
    }

    let mut out = Vec::with_capacity(by_cell.len());
    for ((scenario, broker_count), cell_runs) in by_cell {
        let partitions = cell_runs.first().map_or(0, |r| r.topology.partitions);
        let agg_stack = |stack: Stack| -> StackAgg {
            let stack_runs: Vec<&RunOutput> = cell_runs
                .iter()
                .copied()
                .filter(|r| r.stack == stack)
                .collect();
            let mut by_metric = BTreeMap::new();
            for m in &metrics {
                let vals: Vec<f64> = stack_runs.iter().map(|r| m.value(r)).collect();
                by_metric.insert(m.key, stat(&vals));
            }
            StackAgg {
                n_runs: stack_runs.len(),
                metrics: by_metric,
            }
        };
        out.push(CellAgg {
            scenario,
            partitions,
            broker_count,
            crabka: agg_stack(Stack::Crabka),
            kafka: agg_stack(Stack::Kafka),
        });
    }
    out
}

/// One averaged point of a time series. It holds the across-run mean of a metric
/// at a fixed offset into the run, and how many runs gave that offset.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TsPoint {
    pub t_offset_ms: TimeOffsetMs,
    pub mean: f64,
    pub n: usize,
}

/// An across-run-averaged time series for one `(cell, stack, metric)`.
#[derive(Debug, Clone)]
pub struct TsSeries {
    pub scenario: String,
    pub broker_count: u32,
    pub stack: Stack,
    pub metric: &'static str,
    pub points: Vec<TsPoint>,
}

type SampleExtract = fn(&Sample) -> f64;
type BrokerExtract = fn(&BrokerSample) -> f64;

fn client_ts_metrics() -> Vec<(&'static str, SampleExtract)> {
    vec![
        ("producer_msgs_per_sec", |s| s.producer_rate.per_sec_f64()),
        ("consumer_msgs_per_sec", |s| s.consumer_rate.per_sec_f64()),
        ("producer_p99_ms", |s| millis_f64(s.producer_p99)),
        ("consumer_e2e_p99_ms", |s| millis_f64(s.consumer_e2e_p99)),
    ]
}

fn broker_ts_metrics() -> Vec<(&'static str, BrokerExtract)> {
    vec![
        ("broker_cpu_cores", |b| b.cpu_cores),
        ("broker_mem_working_set_mb", |b| {
            mebibytes_f64(b.mem_working_set)
        }),
    ]
}

/// For each `(scenario, broker_count, stack, metric)`, averages the per-interval
/// samples across all runs at each shared time offset. A ragged run that ended
/// early gives fewer points to the tail offsets. `TsPoint::n` records how many
/// runs backed each averaged point.
#[must_use]
pub fn averaged_timeseries(runs: &[RunOutput]) -> Vec<TsSeries> {
    let client_metrics = client_ts_metrics();
    let broker_metrics = broker_ts_metrics();

    let mut by_cell: BTreeMap<(String, u32), Vec<&RunOutput>> = BTreeMap::new();
    for r in runs {
        by_cell
            .entry((r.scenario.name.clone(), r.topology.broker_count))
            .or_default()
            .push(r);
    }

    let mut out = Vec::new();
    for ((scenario, broker_count), cell_runs) in by_cell {
        for stack in [Stack::Crabka, Stack::Kafka] {
            let stack_runs: Vec<&RunOutput> = cell_runs
                .iter()
                .copied()
                .filter(|r| r.stack == stack)
                .collect();
            if stack_runs.is_empty() {
                continue;
            }

            for &(key, extract) in &client_metrics {
                let mut buckets: BTreeMap<TimeOffsetMs, Vec<f64>> = BTreeMap::new();
                for r in &stack_runs {
                    for s in &r.samples {
                        buckets.entry(s.t_offset_ms).or_default().push(extract(s));
                    }
                }
                if let Some(series) =
                    series_from_buckets(&scenario, broker_count, stack, key, buckets)
                {
                    out.push(series);
                }
            }

            for &(key, extract) in &broker_metrics {
                let mut buckets: BTreeMap<TimeOffsetMs, Vec<f64>> = BTreeMap::new();
                for r in &stack_runs {
                    for b in &r.broker_samples {
                        buckets.entry(b.t_offset_ms).or_default().push(extract(b));
                    }
                }
                if let Some(series) =
                    series_from_buckets(&scenario, broker_count, stack, key, buckets)
                {
                    out.push(series);
                }
            }
        }
    }
    out
}

/// Averages each time-offset bucket across runs into a [`TsSeries`]. Returns
/// `None` when no run in the group produced any sample for this metric.
fn series_from_buckets(
    scenario: &str,
    broker_count: u32,
    stack: Stack,
    metric: &'static str,
    buckets: BTreeMap<TimeOffsetMs, Vec<f64>>,
) -> Option<TsSeries> {
    if buckets.is_empty() {
        return None;
    }
    let points = buckets
        .into_iter()
        .map(|(t_offset_ms, vals)| {
            let s = stat(&vals);
            TsPoint {
                t_offset_ms,
                mean: s.mean,
                n: s.n,
            }
        })
        .collect();
    Some(TsSeries {
        scenario: scenario.to_string(),
        broker_count,
        stack,
        metric,
        points,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ids::WallclockMs,
        scenario::{
            Acks, Compression, LatencyPercentiles, LoadMode, ModeTag, Resource, Scenario,
            Throughput, Topology,
        },
    };

    fn run(
        stack: Stack,
        scenario: &str,
        broker_count: u32,
        producer_rate: Frequency,
        samples: Vec<Sample>,
        broker_samples: Vec<BrokerSample>,
    ) -> RunOutput {
        RunOutput {
            scenario: Scenario {
                name: scenario.into(),
                mode_tag: ModeTag::Cluster,
                msg_size: bytes(100),
                key_size: ByteSize::ZERO,
                partitions: 100,
                replication_factor: 3,
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
                partitions: 100,
                replication_factor: 3,
                broker_count,
            },
            wallclock_start_unix_ms: WallclockMs(0),
            wallclock_end_unix_ms: WallclockMs(60_000),
            throughput: Throughput {
                producer_rate,
                ..Throughput::default()
            },
            producer_latency: LatencyPercentiles::default(),
            consumer_e2e_latency: LatencyPercentiles::default(),
            resource: Resource::default(),
            disturbance: None,
            startup: None,
            first_ack: Time::ZERO,
            errors: vec![],
            notes: vec![],
            samples,
            broker_samples,
        }
    }

    fn sample(t: u64, producer_rate: Frequency) -> Sample {
        Sample {
            t_offset_ms: TimeOffsetMs(t),
            producer_rate,
            ..Sample::default()
        }
    }

    #[test]
    fn stat_computes_mean_and_sample_stddev() {
        let s = stat(&[2.0, 4.0, 6.0]);
        assert2::assert!(s.n == 3);
        assert2::assert!((s.mean - 4.0).abs() < f64::EPSILON);
        // sample variance = ((−2)²+0²+2²)/(3−1) = 8/2 = 4 → stddev 2.
        assert2::assert!((s.stddev - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn stat_single_and_empty() {
        assert2::assert!(
            stat(&[5.0])
                == Stat {
                    mean: 5.0,
                    stddev: 0.0,
                    n: 1
                }
        );
        assert2::assert!(stat(&[]) == Stat::default());
    }

    #[test]
    fn cv_percent_is_zero_when_mean_zero() {
        assert2::assert!(stat(&[0.0, 0.0]).cv_percent().abs() < f64::EPSILON);
        let s = stat(&[100.0, 200.0, 300.0]); // mean 200, stddev 100 → cv 50%
        assert2::assert!((s.cv_percent() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_cells_groups_and_averages_each_stack() {
        let runs = vec![
            run(Stack::Crabka, "sat", 6, per_sec(100), vec![], vec![]),
            run(Stack::Crabka, "sat", 6, per_sec(200), vec![], vec![]),
            run(Stack::Crabka, "sat", 6, per_sec(300), vec![], vec![]),
            run(Stack::Kafka, "sat", 6, per_sec(50), vec![], vec![]),
            run(Stack::Kafka, "sat", 6, per_sec(50), vec![], vec![]),
        ];
        let cells = aggregate_cells(&runs);
        assert2::assert!(cells.len() == 1);
        let c = &cells[0];
        assert2::assert!(c.scenario.as_str() == "sat");
        assert2::assert!(c.broker_count == 6);
        assert2::assert!(c.partitions == 100);
        assert2::assert!(c.crabka.n_runs == 3);
        assert2::assert!(c.kafka.n_runs == 2);
        let cm = c.crabka.metrics["producer_msgs_per_sec"];
        assert2::assert!((cm.mean - 200.0).abs() < f64::EPSILON);
        assert2::assert!((cm.stddev - 100.0).abs() < f64::EPSILON);
        let km = c.kafka.metrics["producer_msgs_per_sec"];
        assert2::assert!((km.mean - 50.0).abs() < f64::EPSILON);
        assert2::assert!(km.stddev.abs() < f64::EPSILON);
        assert2::assert!(km.n == 2);
    }

    #[test]
    fn aggregate_cells_separates_topologies() {
        let runs = vec![
            run(Stack::Crabka, "sat", 3, per_sec(10), vec![], vec![]),
            run(Stack::Crabka, "sat", 6, per_sec(20), vec![], vec![]),
        ];
        let cells = aggregate_cells(&runs);
        assert2::assert!(cells.len() == 2);
        assert2::assert!(cells[0].broker_count == 3);
        assert2::assert!(cells[1].broker_count == 6);
    }

    #[test]
    fn averaged_timeseries_averages_across_runs_per_offset() {
        let runs = vec![
            run(
                Stack::Crabka,
                "sat",
                6,
                Frequency::ZERO,
                vec![sample(0, per_sec(1000)), sample(2000, per_sec(2000))],
                vec![BrokerSample {
                    t_offset_ms: TimeOffsetMs(0),
                    cpu_cores: 2.0,
                    mem_working_set: mebibytes(1),
                }],
            ),
            run(
                Stack::Crabka,
                "sat",
                6,
                Frequency::ZERO,
                vec![sample(0, per_sec(3000)), sample(2000, per_sec(4000))],
                vec![BrokerSample {
                    t_offset_ms: TimeOffsetMs(0),
                    cpu_cores: 4.0,
                    mem_working_set: mebibytes(3),
                }],
            ),
        ];
        let series = averaged_timeseries(&runs);
        let prod = series
            .iter()
            .find(|s| s.metric == "producer_msgs_per_sec" && s.stack == Stack::Crabka)
            .expect("producer series present");
        assert2::assert!(
            prod.points
                == vec![
                    TsPoint {
                        t_offset_ms: TimeOffsetMs(0),
                        mean: 2000.0,
                        n: 2
                    },
                    TsPoint {
                        t_offset_ms: TimeOffsetMs(2000),
                        mean: 3000.0,
                        n: 2
                    },
                ]
        );
        let cpu = series
            .iter()
            .find(|s| s.metric == "broker_cpu_cores")
            .expect("broker cpu series present");
        assert2::assert!(
            cpu.points[0]
                == TsPoint {
                    t_offset_ms: TimeOffsetMs(0),
                    mean: 3.0,
                    n: 2
                }
        );
        // mem in MiB: (1 + 3) / 2 = 2.0
        let mem = series
            .iter()
            .find(|s| s.metric == "broker_mem_working_set_mb")
            .expect("broker mem series present");
        assert2::assert!((mem.points[0].mean - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn averaged_timeseries_handles_ragged_runs() {
        let runs = vec![
            run(
                Stack::Kafka,
                "sat",
                6,
                Frequency::ZERO,
                vec![sample(0, per_sec(10)), sample(2000, per_sec(20))],
                vec![],
            ),
            run(
                Stack::Kafka,
                "sat",
                6,
                Frequency::ZERO,
                vec![sample(0, per_sec(30))],
                vec![],
            ),
        ];
        let series = averaged_timeseries(&runs);
        let prod = series
            .iter()
            .find(|s| s.metric == "producer_msgs_per_sec" && s.stack == Stack::Kafka)
            .expect("series");
        assert2::assert!(
            prod.points[0]
                == TsPoint {
                    t_offset_ms: TimeOffsetMs(0),
                    mean: 20.0,
                    n: 2
                }
        );
        // Only one run reached t=2000.
        assert2::assert!(
            prod.points[1]
                == TsPoint {
                    t_offset_ms: TimeOffsetMs(2000),
                    mean: 20.0,
                    n: 1
                }
        );
    }
}
