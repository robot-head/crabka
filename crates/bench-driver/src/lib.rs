// Benchmark drivers do a lot of `as u64` on Unix nanos / millis where
// precision loss past i64::MAX is irrelevant; silence pedantic casts
// at the crate root rather than at each call site.
#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::similar_names
)]

//! Load driver + report aggregator for the Crabka vs Strimzi benchmark
//! harness on Kubernetes.
//!
//! See `bench/README.md` and the plan at
//! `/root/.claude/plans/build-a-comprehensive-benchmark-humming-thimble.md`.
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

pub mod failover;
pub mod hist;
pub mod payload;
pub mod prom;
pub mod rate;
pub mod report;
pub mod scenario;
pub mod workload;
