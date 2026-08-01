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
//! An external-cluster mode ([`external`], `run --external`) points the same
//! workload/measurement/reporting pipeline at any pgwire-speaking SQL system
//! (`CockroachDB`, `YugabyteDB`, `PostgreSQL`, a remote crabka cluster) without
//! launching crabka processes; faults are unavailable there, and resource
//! sampling covers local processes discovered by listening port (or named
//! via `--external-pids`).
//!
//! Every magnitude the harness handles — run and warmup lengths, fault offsets
//! and durations, target rates, injected delays, bandwidth caps, measured
//! latencies, sampled RSS — is a [`crabka_units`] quantity rather than a bare
//! number, so the scenario YAML and the JSON report carry their units and the
//! conversions happen only at the seams (`tokio` timers, `/proc` reads, the
//! `crabka-gres` command line).
//!
//! Module map:
//! - [`scenario`] — the YAML schema and its validation.
//! - [`cluster`] — broker + node process orchestration and tenant provisioning.
//! - [`external`] — external-endpoint parsing, validation, pid discovery.
//! - [`proxy`] — the chaos TCP proxy every endpoint sits behind.
//! - [`workload`] — the SQL load driver (mix, pacing, latency histograms).
//! - [`faults`] — executes a scenario's fault timeline against a live cluster.
//! - [`metrics`] — per-process CPU/RSS sampling via `/proc`.
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
