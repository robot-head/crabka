//! Scenario-driven scalability and fault-injection harness for crabka-gres.
//!
//! The harness boots a real multi-process cluster — one `crabka-broker` child
//! plus N `crabka-gres` compute nodes — with every inter-node and client-facing
//! TCP endpoint fronted by a [`proxy::ChaosProxy`]. A YAML [`scenario::Scenario`]
//! describes the topology, the timestamp-source mode (Percolator-style
//! `LogicalTso` or hybrid logical clock), the SQL workload mix, and a timeline
//! of network faults (partitions, latency, throttling, node kills, flapping
//! links). The run produces a [`report::RunReport`] as JSON plus a Markdown
//! summary; `compare` runs the same scenario under both timestamp modes and
//! renders them side by side.
//!
//! Module map:
//! - [`scenario`] — the YAML schema and its validation.
//! - [`cluster`] — broker + node process orchestration and tenant provisioning.
//! - [`proxy`] — the chaos TCP proxy every endpoint sits behind.
//! - [`workload`] — the SQL load driver (mix, pacing, latency histograms).
//! - [`faults`] — executes a scenario's fault timeline against a live cluster.
//! - [`metrics`] — per-process CPU/RSS sampling via `/proc`.
//! - [`report`] — serialisable run results and Markdown rendering.
//! - [`runner`] — ties the above together for one scenario run.

pub mod cluster;
pub mod faults;
pub mod metrics;
pub mod proxy;
pub mod report;
pub mod runner;
pub mod scenario;
pub mod workload;
