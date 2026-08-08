//! Scenario-driven scalability and fault-injection harness for crabka-gres.
//!
//! The harness boots a real multi-process cluster: one `crabka-broker` child
//! and N `crabka-gres` compute nodes. A [`proxy::ChaosProxy`] fronts every
//! inter-node and client-facing TCP endpoint. A YAML [`scenario::Scenario`]
//! describes the topology, the timestamp-source mode, which is either the
//! Percolator-style `LogicalTso` or a hybrid logical clock, the SQL workload
//! mix, and a timeline of network faults. Those faults are partitions,
//! latency, throttling, node kills, and flapping links. A run produces a
//! [`report::RunReport`] as JSON and a Markdown summary. `compare` runs the
//! same scenario under both timestamp modes and renders them side by side.
//!
//! An external-cluster mode, [`external`] under `run --external`, points the
//! same workload, measurement, and reporting pipeline at any pgwire-speaking
//! SQL system, such as `CockroachDB`, `YugabyteDB`, `PostgreSQL`, or a remote
//! crabka cluster. It launches no crabka process. Faults are unavailable
//! there, and resource sampling covers the local processes found by listening
//! port, or named with `--external-pids`.
//!
//! Every magnitude the harness handles is a [`crabka_units`] quantity and not
//! a bare number. Those magnitudes are the run and warmup lengths, the fault
//! offsets and durations, the target rates, the injected delays, the bandwidth
//! caps, the measured latencies, and the sampled RSS. The scenario YAML and
//! the JSON report therefore carry their units, and the conversions happen
//! only at the seams: the `tokio` timers, the `/proc` reads, and the
//! `crabka-gres` command line.
//!
//! Module map:
//! - [`scenario`] — the YAML schema and its validation.
//! - [`cluster`] — broker and node process orchestration, and tenant
//!   provisioning.
//! - [`external`] — external-endpoint parsing, validation, and pid discovery.
//! - [`proxy`] — the chaos TCP proxy that every endpoint sits behind.
//! - [`workload`] — the SQL load driver: mix, pacing, and latency histograms.
//! - [`faults`] — executes a scenario's fault timeline against a live cluster.
//! - [`metrics`] — per-process CPU and RSS sampling through `/proc`.
//! - [`report`] — serialisable run results and Markdown rendering.
//! - [`runner`] — ties the above together for one scenario run.

pub mod cluster;
pub mod config;
pub mod external;
pub mod faults;
pub mod metrics;
pub mod proxy;
pub mod report;
pub mod runner;
pub mod scenario;
pub mod workload;
