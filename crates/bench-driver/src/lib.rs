//! Load driver and report aggregator for the Crabka vs Strimzi benchmark
//! harness on Kubernetes.
//!
//! Scenarios describe a target Kafka stack, a workload shape, and optional
//! disturbance windows. The driver applies the scenario to Kubernetes, runs
//! the producer/consumer load, samples Prometheus, and writes a `RunOutput`
//! JSON artifact. The report binary reads those artifacts back and renders
//! the side-by-side Markdown summary used in benchmark reports.
//!
//! The crate ships two binaries:
//!
//! - `crabka-bench-driver` — runs one scenario against one Kafka stack,
//!   either Crabka or Strimzi/Kafka. It captures throughput, latency, and
//!   disturbance data, queries Prometheus for resource usage, and writes a
//!   single `RunOutput` JSON file.
//! - `crabka-bench-report` — walks a directory of `RunOutput` files,
//!   groups them by scenario name, and writes a side-by-side Markdown
//!   summary.
//!
//! ## Dimensioned values
//!
//! Sizes, durations, and rates are [`crabka_units`] quantities throughout, and
//! not bare numbers. A scenario's `msg_size` is a `ByteSize`, its `linger` and
//! `duration` are a `Time`, and a paced run's `rate` is a `Frequency`. The
//! operator writes them with units (`512B`, `5ms`, `20000/s`), and the measured
//! `RunOutput` encodes them as exact integers. See [`scenario`] for the
//! encoding of each field and [`docs/uom-adoption.md`] for the vocabulary.
//!
//! [`docs/uom-adoption.md`]: https://github.com/robot-head/crabka/blob/main/docs/uom-adoption.md
//!
//! ## Command-line workflow
//!
//! ```text
//! crabka-bench-driver \
//!   --scenario bench/scenarios/small-msg-saturate.yaml \
//!   --stack crabka \
//!   --namespace kafka-bench \
//!   --out runs/crabka-steady.json
//!
//! crabka-bench-report --input runs --out report.md
//! ```
//!
//! ## Programmatic report aggregation
//!
//! ```no_run
//! use std::path::Path;
//!
//! use crabka_bench_driver::report;
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let markdown = report::render_markdown(Path::new("runs"), true)?;
//! std::fs::write("report.md", markdown)?;
//! # Ok(())
//! # }
//! ```

pub mod aggregate;
pub mod failover;
pub mod graph;
pub mod hist;
pub mod ids;
mod numeric;
pub mod payload;
pub mod prom;
pub mod rate;
pub mod report;
pub mod scenario;
pub mod workload;
