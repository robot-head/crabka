// Benchmark drivers do a lot of `as u64` on Unix nanos / millis where
// precision loss past i64::MAX is irrelevant; silence pedantic casts
// at the crate root rather than at each call site. Likewise,
// `format!(...)` appended to a building Markdown string reads more
// linearly than the equivalent `write!(out, ...).unwrap()` chain, and
// the orchestrator function in `workload::run` is intentionally one
// long top-to-bottom narrative.
#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::similar_names,
    clippy::format_push_string,
    clippy::too_many_lines
)]

//! Load driver + report aggregator for the Crabka vs Strimzi benchmark
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
//! - `crabka-bench-driver` — runs one scenario against one Kafka stack
//!   (Crabka or Strimzi/Kafka), captures throughput / latency / disturbance
//!   data, queries Prometheus for resource usage, and writes a single
//!   `RunOutput` JSON file.
//! - `crabka-bench-report` — walks a directory of `RunOutput` files,
//!   groups them by scenario name, and emits a side-by-side Markdown
//!   summary.
//!
//! ## Command-line workflow
//!
//! ```text
//! crabka-bench-driver \
//!   --scenario bench/scenarios/steady_1p1r.toml \
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
pub mod payload;
pub mod prom;
pub mod rate;
pub mod report;
pub mod scenario;
pub mod workload;
