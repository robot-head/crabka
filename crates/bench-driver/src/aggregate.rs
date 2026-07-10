//! Cross-run aggregation — the "accurate averages" layer.
//!
//! A benchmark *cell* is one `(scenario name, broker_count)`. Each cell is run
//! `N` times per stack; this module reduces those `N` [`RunOutput`]s into a
//! mean ± sample-stddev per metric, and averages the per-interval time series
//! across runs at each time offset. Both the Markdown summary and the Plotly
//! graphs read these aggregates, so "the average" is computed in exactly one
//! place rather than re-derived ad hoc per renderer.

use std::collections::BTreeMap;

use crate::{
    ids::TimeOffsetMs,
    scenario::{BrokerSample, RunOutput, Sample, Stack},
};

/// Mean / spread of a single scalar metric over the runs of one `(cell, stack)`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Stat {
    pub mean: f64,
    /// Sample (Bessel-corrected) standard deviation; `0.0` for fewer than two
    /// runs (a single run has no measurable spread).
    pub stddev: f64,
    pub n: usize,
}

impl Stat {
    /// Coefficient of variation as a percentage (`stddev ÷ mean × 100`); `0`
    /// when the mean is `0` so an absent/zero metric renders cleanly.
    #[must_use]
    pub fn cv_percent(&self) -> f64 {
        if self.mean == 0.0 {
            0.0
        } else {
            (self.stddev / self.mean).abs() * 100.0
        }
    }
}

/// Mean + sample stddev + count over a sample of run-level values.
#[must_use]
pub fn stat(values: &[f64]) -> Stat {
    let n = values.len();
    if n == 0 {
        return Stat::default();
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let stddev = if n < 2 {
        0.0
    } else {
        let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0);
        var.sqrt()
    };
    Stat { mean, stddev, n }
}

/// A headline scalar metric: its key, how to pull it from a run, and which
/// direction counts as "better" (so a renderer can colour/ratio correctly).
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
            extract: |r| r.throughput.producer_msgs_per_sec,
        },
        ScalarMetric {
            key: "consumer_msgs_per_sec",
            label: "Consumer throughput",
            unit: "msgs/s",
            higher_better: true,
            extract: |r| r.throughput.consumer_msgs_per_sec,
        },
        ScalarMetric {
            key: "producer_p99_ms",
            label: "Producer ack p99",
            unit: "ms",
            higher_better: false,
            extract: |r| r.producer_latency_ms.p99_ms,
        },
        ScalarMetric {
            key: "consumer_e2e_p99_ms",
            label: "Consumer e2e p99",
            unit: "ms",
            higher_better: false,
            extract: |r| r.consumer_e2e_latency_ms.p99_ms,
        },
        ScalarMetric {
            key: "msgs_per_cpu_core",
            label: "Efficiency",
            unit: "msgs/s per CPU-core",
            higher_better: true,
            extract: |r| r.resource.msgs_per_cpu_core,
        },
        ScalarMetric {
            key: "mem_working_set_mb",
            label: "Broker working set",
            unit: "MiB",
            higher_better: false,
            extract: |r| r.resource.mem_cgroup_working_set_bytes as f64 / 1_048_576.0,
        },
    ]
}

/// Per-metric aggregates for one stack within a cell.
#[derive(Debug, Clone, Default)]
pub struct StackAgg {
    pub n_runs: usize,
    /// metric key → [`Stat`] across this stack's runs in the cell.
    pub metrics: BTreeMap<&'static str, Stat>,
}

/// One benchmark cell: a `(scenario, topology)` with crabka and kafka
/// aggregates side by side.
#[derive(Debug, Clone)]
pub struct CellAgg {
    pub scenario: String,
    pub partitions: i32,
    pub broker_count: u32,
    pub crabka: StackAgg,
    pub kafka: StackAgg,
}

/// Group runs by `(scenario, broker_count)` and reduce each stack's runs to a
/// per-metric [`Stat`]. Cells are returned in `(scenario, broker_count)` order.
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

/// One averaged point of a time series: the across-run mean of a metric at a
/// fixed offset into the run, plus how many runs contributed that offset.
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
        ("producer_msgs_per_sec", |s| s.producer_msgs_per_sec),
        ("consumer_msgs_per_sec", |s| s.consumer_msgs_per_sec),
        ("producer_p99_ms", |s| s.producer_p99_ms),
        ("consumer_e2e_p99_ms", |s| s.consumer_e2e_p99_ms),
    ]
}

fn broker_ts_metrics() -> Vec<(&'static str, BrokerExtract)> {
    vec![
        ("broker_cpu_cores", |b| b.cpu_cores),
        ("broker_mem_working_set_mb", |b| {
            b.mem_working_set_bytes as f64 / 1_048_576.0
        }),
    ]
}

/// For each `(scenario, broker_count, stack, metric)`, average the per-interval
/// samples across all runs at each shared time offset. Ragged runs (one ended
/// early) simply contribute fewer points to the tail offsets; `TsPoint::n`
/// records how many runs backed each averaged point.
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

/// Average each time-offset bucket across runs into a [`TsSeries`], or `None`
/// when no run in the group produced any sample for this metric.
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
        prod_mps: f64,
        samples: Vec<Sample>,
        broker_samples: Vec<BrokerSample>,
    ) -> RunOutput {
        RunOutput {
            scenario: Scenario {
                name: scenario.into(),
                mode_tag: ModeTag::Cluster,
                msg_size_bytes: 100,
                key_size_bytes: 0,
                partitions: 100,
                replication_factor: 3,
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
                partitions: 100,
                replication_factor: 3,
                broker_count,
            },
            wallclock_start_unix_ms: WallclockMs(0),
            wallclock_end_unix_ms: WallclockMs(60_000),
            throughput: Throughput {
                producer_msgs_per_sec: prod_mps,
                ..Throughput::default()
            },
            producer_latency_ms: LatencyPercentiles::default(),
            consumer_e2e_latency_ms: LatencyPercentiles::default(),
            resource: Resource::default(),
            disturbance: None,
            startup_ms: None,
            first_ack_ms: 0,
            errors: vec![],
            notes: vec![],
            samples,
            broker_samples,
        }
    }

    fn sample(t: u64, prod_mps: f64) -> Sample {
        Sample {
            t_offset_ms: TimeOffsetMs(t),
            producer_msgs_per_sec: prod_mps,
            consumer_msgs_per_sec: 0.0,
            producer_p50_ms: 0.0,
            producer_p99_ms: 0.0,
            consumer_e2e_p99_ms: 0.0,
        }
    }

    #[test]
    fn stat_computes_mean_and_sample_stddev() {
        let s = stat(&[2.0, 4.0, 6.0]);
        assert_eq!(s.n, 3);
        assert_eq!(s.mean, 4.0);
        // sample variance = ((−2)²+0²+2²)/(3−1) = 8/2 = 4 → stddev 2.
        assert_eq!(s.stddev, 2.0);
    }

    #[test]
    fn stat_single_and_empty() {
        assert_eq!(
            stat(&[5.0]),
            Stat {
                mean: 5.0,
                stddev: 0.0,
                n: 1
            }
        );
        assert_eq!(stat(&[]), Stat::default());
    }

    #[test]
    fn cv_percent_is_zero_when_mean_zero() {
        assert_eq!(stat(&[0.0, 0.0]).cv_percent(), 0.0);
        let s = stat(&[100.0, 200.0, 300.0]); // mean 200, stddev 100 → cv 50%
        assert_eq!(s.cv_percent(), 50.0);
    }

    #[test]
    fn aggregate_cells_groups_and_averages_each_stack() {
        let runs = vec![
            run(Stack::Crabka, "sat", 6, 100.0, vec![], vec![]),
            run(Stack::Crabka, "sat", 6, 200.0, vec![], vec![]),
            run(Stack::Crabka, "sat", 6, 300.0, vec![], vec![]),
            run(Stack::Kafka, "sat", 6, 50.0, vec![], vec![]),
            run(Stack::Kafka, "sat", 6, 50.0, vec![], vec![]),
        ];
        let cells = aggregate_cells(&runs);
        assert_eq!(cells.len(), 1);
        let c = &cells[0];
        assert_eq!(
            (c.scenario.as_str(), c.broker_count, c.partitions),
            ("sat", 6, 100)
        );
        assert_eq!((c.crabka.n_runs, c.kafka.n_runs), (3, 2));
        let cm = c.crabka.metrics["producer_msgs_per_sec"];
        assert_eq!((cm.mean, cm.stddev), (200.0, 100.0));
        let km = c.kafka.metrics["producer_msgs_per_sec"];
        assert_eq!((km.mean, km.stddev, km.n), (50.0, 0.0, 2));
    }

    #[test]
    fn aggregate_cells_separates_topologies() {
        let runs = vec![
            run(Stack::Crabka, "sat", 3, 10.0, vec![], vec![]),
            run(Stack::Crabka, "sat", 6, 20.0, vec![], vec![]),
        ];
        let cells = aggregate_cells(&runs);
        assert_eq!(cells.len(), 2);
        assert_eq!((cells[0].broker_count, cells[1].broker_count), (3, 6));
    }

    #[test]
    fn averaged_timeseries_averages_across_runs_per_offset() {
        let runs = vec![
            run(
                Stack::Crabka,
                "sat",
                6,
                0.0,
                vec![sample(0, 1000.0), sample(2000, 2000.0)],
                vec![BrokerSample {
                    t_offset_ms: TimeOffsetMs(0),
                    cpu_cores: 2.0,
                    mem_working_set_bytes: 1_048_576,
                }],
            ),
            run(
                Stack::Crabka,
                "sat",
                6,
                0.0,
                vec![sample(0, 3000.0), sample(2000, 4000.0)],
                vec![BrokerSample {
                    t_offset_ms: TimeOffsetMs(0),
                    cpu_cores: 4.0,
                    mem_working_set_bytes: 3_145_728,
                }],
            ),
        ];
        let series = averaged_timeseries(&runs);
        let prod = series
            .iter()
            .find(|s| s.metric == "producer_msgs_per_sec" && s.stack == Stack::Crabka)
            .expect("producer series present");
        assert_eq!(
            prod.points,
            vec![
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
        assert_eq!(
            cpu.points[0],
            TsPoint {
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
        assert_eq!(mem.points[0].mean, 2.0);
    }

    #[test]
    fn averaged_timeseries_handles_ragged_runs() {
        let runs = vec![
            run(
                Stack::Kafka,
                "sat",
                6,
                0.0,
                vec![sample(0, 10.0), sample(2000, 20.0)],
                vec![],
            ),
            run(Stack::Kafka, "sat", 6, 0.0, vec![sample(0, 30.0)], vec![]),
        ];
        let series = averaged_timeseries(&runs);
        let prod = series
            .iter()
            .find(|s| s.metric == "producer_msgs_per_sec" && s.stack == Stack::Kafka)
            .expect("series");
        assert_eq!(
            prod.points[0],
            TsPoint {
                t_offset_ms: TimeOffsetMs(0),
                mean: 20.0,
                n: 2
            }
        );
        // Only one run reached t=2000.
        assert_eq!(
            prod.points[1],
            TsPoint {
                t_offset_ms: TimeOffsetMs(2000),
                mean: 20.0,
                n: 1
            }
        );
    }
}
