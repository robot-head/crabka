//! The YAML scenario schema: topology, timestamp mode, workload, fault timeline.
//!
//! A scenario is the single input to a harness run. [`Scenario::from_yaml_file`]
//! parses and validates it; every other module consumes the parsed types.
//!
//! Every dimensioned field is a [`crabka_units`] quantity and carries its unit
//! in the YAML — `duration: 60s`, `rate: { fixed: { target_rate: 500/s } }`,
//! `throttle: { rate: 128KiB/s }`. A bare number is rejected rather than
//! guessed at, which is the whole point of the types.

use std::{collections::BTreeMap, fmt, path::Path, str::FromStr};

use crabka_units::{
    fmt::Human as _,
    prelude::*,
    serde_units::human::{byte_rate, frequency, option_time, time},
};
use serde::{Deserialize, Serialize};

/// A complete harness scenario as parsed from YAML.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Scenario identifier used in report file names.
    pub name: String,
    /// Human-readable summary shown in reports.
    #[serde(default)]
    pub description: String,
    /// Cluster shape to launch.
    pub topology: TopologySpec,
    /// Timestamp-source mode for the tenant. Overridable from the CLI.
    #[serde(default, with = "serde_yaml::with::singleton_map")]
    pub mode: ModeSpec,
    /// SQL load to drive.
    pub workload: WorkloadSpec,
    /// Timeline of faults injected while the workload runs. Offsets are
    /// relative to the start of the measurement window (after warmup).
    #[serde(default)]
    pub faults: Vec<FaultEvent>,
}

/// Cluster shape: how many compute nodes and ranges to launch.
///
/// Ranges are assigned to nodes round-robin: range `r` is hosted by node
/// `r % nodes`, so range 0 (catalog + coordinator + timestamp authority)
/// always lives on node 0.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySpec {
    /// Number of `crabka-gres` compute processes.
    pub nodes: u16,
    /// Number of ranges (r0..rN-1). Range 0 is the coordinator range.
    pub ranges: u16,
    /// Per-node HLC wall-clock skew (node index → signed offset), passed to
    /// `--hlc-wall-offset-ms`. Only meaningful in HLC mode; today only the
    /// skew of the node hosting range 0 (the single HLC authority) is
    /// observable.
    #[serde(default, with = "skew_map")]
    pub clock_skew: BTreeMap<u16, Time>,
    /// Pin each node to this many dedicated CPUs (the broker gets its own
    /// slice first — see `broker_cpus` — and nodes get disjoint slices
    /// after it). On a single machine this makes each node behave like a
    /// fixed-capacity host, so adding nodes adds real compute and scaling
    /// curves measure the architecture rather than one box being
    /// partitioned N ways. Launch fails if the machine has fewer CPUs than
    /// `broker_cpus + nodes * cpus_per_node`. For full isolation run the
    /// harness binary itself under `taskset` on the leftover CPUs.
    #[serde(default)]
    pub cpus_per_node: Option<u32>,
    /// CPUs pinned to the broker when `cpus_per_node` pinning is active
    /// (default 2). Raise it to test whether the shared WAL broker is the
    /// scaling ceiling. Only meaningful together with `cpus_per_node`.
    #[serde(default)]
    pub broker_cpus: Option<u32>,
}

/// A node-indexed map of clock-skew extents, written as human time strings
/// (`clock_skew: { 0: 400ms, 2: -100ms }`).
///
/// The per-quantity `#[serde(with = ...)]` modules cover a single value, not a
/// map of them, so the map's own adapter lives here and reuses the same
/// parse/render pair.
mod skew_map {
    use std::collections::BTreeMap;

    use crabka_units::{Time, fmt::Human as _, parse};
    use serde::{
        Deserialize as _, Deserializer, Serializer,
        de::Error as _,
        ser::{Error as _, SerializeMap as _},
    };

    /// Writes each entry's extent as its human string form.
    ///
    /// # Errors
    ///
    /// Whatever the serializer reports for a map of strings.
    pub fn serialize<S: Serializer>(
        value: &BTreeMap<u16, Time>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(value.len()))?;
        for (node, skew) in value {
            // `serialize_entry` would need the value pre-rendered anyway; a
            // `String` keeps the key numeric, which YAML flow maps expect.
            map.serialize_entry(node, &skew.human().to_string())
                .map_err(S::Error::custom)?;
        }
        map.end()
    }

    /// Reads each entry's extent from its human string form.
    ///
    /// # Errors
    ///
    /// If a value is not a time extent with an explicit unit.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<u16, Time>, D::Error> {
        BTreeMap::<u16, String>::deserialize(deserializer)?
            .into_iter()
            .map(|(node, raw)| {
                parse::time(&raw)
                    .map(|skew| (node, skew))
                    .map_err(D::Error::custom)
            })
            .collect()
    }
}

/// Timestamp-source mode for the tenant under test.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ModeSpec {
    /// Centralized Percolator-style logical timestamp oracle on range 0.
    #[default]
    LogicalTso,
    /// Hybrid logical clock.
    Hlc {
        /// Uncertainty window (`max_offset`).
        #[serde(default = "default_hlc_max_offset", with = "time")]
        max_offset: Time,
    },
}

fn default_hlc_max_offset() -> Time {
    millis(250)
}

/// No extent, for optional duration fields that default to zero.
fn zero_time() -> Time {
    Time::ZERO
}

impl fmt::Display for ModeSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogicalTso => write!(f, "logical-tso"),
            Self::Hlc { max_offset } => write!(f, "hlc(max_offset={})", max_offset.human()),
        }
    }
}

/// The SQL workload to drive against the cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    /// Concurrent client connections, spread round-robin over the nodes'
    /// SQL front doors.
    pub connections: u32,
    /// Pacing: saturate or a fixed aggregate transaction rate.
    #[serde(default, with = "serde_yaml::with::singleton_map")]
    pub rate: RateSpec,
    /// Load driven before measurement starts.
    #[serde(default = "default_warmup", with = "time")]
    pub warmup: Time,
    /// Length of the measured window.
    #[serde(with = "time")]
    pub duration: Time,
    /// Relative weights of the operation classes.
    pub mix: MixSpec,
    /// Number of rows in the contended-update hot table.
    #[serde(default = "default_hot_rows")]
    pub hot_rows: u32,
    /// Zipf exponent for hot-row selection (1.0 = classic Zipf; larger is
    /// more skewed).
    #[serde(default = "default_zipf_exponent")]
    pub zipf_exponent: f64,
}

fn default_warmup() -> Time {
    secs(5)
}

fn default_hot_rows() -> u32 {
    1000
}

fn default_zipf_exponent() -> f64 {
    1.1
}

/// Pacing for the workload.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum RateSpec {
    /// Issue transactions as fast as the cluster accepts them.
    #[default]
    Saturate,
    /// Target a fixed aggregate transaction rate across all connections.
    Fixed {
        /// Transaction rate across the whole workload.
        #[serde(with = "frequency")]
        target_rate: Frequency,
    },
}

/// Relative weights of workload operation classes. Weights need not sum to
/// any particular value; classes with weight 0 are never issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixSpec {
    /// Autocommit single-row insert into a table owned by one range.
    #[serde(default)]
    pub single_shard_insert: u32,
    /// Explicit transaction writing tables on two different ranges (2PC +
    /// global timestamp).
    #[serde(default)]
    pub cross_shard_txn: u32,
    /// Snapshot read touching one range.
    #[serde(default)]
    pub read_only: u32,
    /// Zipf-distributed update of a small hot table (serialization
    /// conflicts and retries).
    #[serde(default)]
    pub contended_update: u32,
}

impl MixSpec {
    /// Sum of all weights.
    #[must_use]
    pub fn total_weight(&self) -> u64 {
        u64::from(self.single_shard_insert)
            + u64::from(self.cross_shard_txn)
            + u64::from(self.read_only)
            + u64::from(self.contended_update)
    }
}

/// One entry in the fault timeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FaultEvent {
    /// Offset after the start of the measurement window.
    #[serde(with = "time")]
    pub at: Time,
    /// The fault to inject.
    #[serde(flatten)]
    pub action: FaultAction,
}

/// A fault to inject at a point in the timeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultAction {
    /// Cut a link for a duration, then heal it.
    Partition {
        /// Which endpoint's proxy to affect.
        target: FaultTarget,
        /// How long until the link heals.
        #[serde(with = "time")]
        duration: Time,
        /// How the cut manifests to peers.
        #[serde(default)]
        style: PartitionStyle,
    },
    /// Add one-way delay to a link for a duration.
    Latency {
        /// Which endpoint's proxy to affect.
        target: FaultTarget,
        /// Base one-way delay, applied in each direction.
        #[serde(with = "time")]
        delay: Time,
        /// Uniform jitter added on top of the base delay.
        #[serde(default = "zero_time", with = "time")]
        jitter: Time,
        /// How long until the delay is removed.
        #[serde(with = "time")]
        duration: Time,
    },
    /// Cap a link's bandwidth for a duration.
    Throttle {
        /// Which endpoint's proxy to affect.
        target: FaultTarget,
        /// Bandwidth cap, per direction.
        #[serde(with = "byte_rate")]
        rate: ByteRate,
        /// How long until the cap is removed.
        #[serde(with = "time")]
        duration: Time,
    },
    /// SIGKILL a node's process, optionally restarting it later.
    KillNode {
        /// Node index to kill.
        node: u16,
        /// How long to wait before restarting the node. `null` leaves it
        /// down.
        #[serde(default, with = "option_time")]
        restart_after: Option<Time>,
    },
    /// Repeatedly partition and heal a link.
    Flap {
        /// Which endpoint's proxy to affect.
        target: FaultTarget,
        /// Length of one on/off half-cycle.
        #[serde(with = "time")]
        period: Time,
        /// How long the link keeps flapping (ends healed).
        #[serde(with = "time")]
        duration: Time,
    },
}

/// How a partition manifests at the TCP level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PartitionStyle {
    /// Packets vanish: live connections stall (no bytes flow, no FIN/RST)
    /// and new connections hang. Models a real network partition; peers see
    /// timeouts. Connections survive if the partition heals before the
    /// application gives up.
    #[default]
    Blackhole,
    /// Live connections are closed and new ones refused. Models an
    /// administratively-down endpoint; peers see immediate errors.
    Reset,
}

/// Which proxied endpoint a fault applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultTarget {
    /// The range-RPC endpoint of one range (inter-node traffic to it).
    Range(u16),
    /// Every range-RPC endpoint.
    AllRanges,
    /// The SQL front door of one node (client traffic to it).
    Sql(u16),
    /// Every node's SQL front door.
    AllSql,
}

impl fmt::Display for FaultTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Range(range) => write!(f, "range:{range}"),
            Self::AllRanges => write!(f, "all-ranges"),
            Self::Sql(node) => write!(f, "sql:{node}"),
            Self::AllSql => write!(f, "all-sql"),
        }
    }
}

/// Error from parsing a [`FaultTarget`] string.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "invalid fault target {input:?}: expected `range:<id>`, `all-ranges`, `sql:<node>`, or `all-sql`"
)]
pub struct FaultTargetParseError {
    input: String,
}

impl FromStr for FaultTarget {
    type Err = FaultTargetParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let invalid = || FaultTargetParseError {
            input: input.to_owned(),
        };
        match input {
            "all-ranges" => Ok(Self::AllRanges),
            "all-sql" => Ok(Self::AllSql),
            _ => {
                if let Some(id) = input.strip_prefix("range:") {
                    id.parse().map(Self::Range).map_err(|_| invalid())
                } else if let Some(node) = input.strip_prefix("sql:") {
                    node.parse().map(Self::Sql).map_err(|_| invalid())
                } else {
                    Err(invalid())
                }
            }
        }
    }
}

impl Serialize for FaultTarget {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for FaultTarget {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// Error from loading or validating a scenario.
#[derive(Debug, thiserror::Error)]
pub enum ScenarioError {
    /// The file could not be read.
    #[error("read scenario {path}: {source}")]
    Read {
        /// Path that failed to load.
        path: String,
        /// Underlying IO error.
        source: std::io::Error,
    },
    /// The YAML did not match the schema.
    #[error("parse scenario {path}: {source}")]
    Parse {
        /// Path that failed to parse.
        path: String,
        /// Underlying YAML error.
        source: serde_yaml::Error,
    },
    /// The scenario parsed but is internally inconsistent.
    #[error("invalid scenario: {0}")]
    Invalid(String),
}

impl Scenario {
    /// Loads and validates a scenario from a YAML file.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioError`] if the file cannot be read, does not match
    /// the schema, or fails [`Scenario::validate`].
    pub fn from_yaml_file(path: &Path) -> Result<Self, ScenarioError> {
        let display = path.display().to_string();
        let raw = std::fs::read_to_string(path).map_err(|source| ScenarioError::Read {
            path: display.clone(),
            source,
        })?;
        let scenario: Self = serde_yaml::from_str(&raw).map_err(|source| ScenarioError::Parse {
            path: display,
            source,
        })?;
        scenario.validate()?;
        Ok(scenario)
    }

    /// Checks internal consistency: non-empty topology and workload, fault
    /// targets within topology bounds, cross-shard weight requiring at
    /// least two ranges.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioError::Invalid`] describing the first problem found.
    pub fn validate(&self) -> Result<(), ScenarioError> {
        let invalid = |message: String| Err(ScenarioError::Invalid(message));
        if self.name.is_empty() {
            return invalid("name must not be empty".to_owned());
        }
        if self.topology.nodes == 0 {
            return invalid("topology.nodes must be at least 1".to_owned());
        }
        if self.topology.ranges == 0 {
            return invalid("topology.ranges must be at least 1".to_owned());
        }
        if self.topology.nodes > self.topology.ranges {
            return invalid(format!(
                "topology.nodes ({}) must not exceed topology.ranges ({}): every node must host \
                 at least one range",
                self.topology.nodes, self.topology.ranges
            ));
        }
        if let Some(node) = self
            .topology
            .clock_skew
            .keys()
            .find(|node| **node >= self.topology.nodes)
        {
            return invalid(format!(
                "clock_skew references node {node} but topology has {} nodes",
                self.topology.nodes
            ));
        }
        if !self.topology.clock_skew.is_empty() && self.mode == ModeSpec::LogicalTso {
            return invalid("clock_skew requires hlc mode".to_owned());
        }
        if self.topology.cpus_per_node == Some(0) {
            return invalid("cpus_per_node must be at least 1 when set".to_owned());
        }
        if self.topology.broker_cpus == Some(0) {
            return invalid("broker_cpus must be at least 1 when set".to_owned());
        }
        if self.topology.broker_cpus.is_some() && self.topology.cpus_per_node.is_none() {
            return invalid("broker_cpus requires cpus_per_node pinning".to_owned());
        }
        if self.workload.connections == 0 {
            return invalid("workload.connections must be at least 1".to_owned());
        }
        if self.workload.duration <= Time::ZERO {
            return invalid("workload.duration must be positive".to_owned());
        }
        if self.workload.mix.total_weight() == 0 {
            return invalid("workload.mix must have at least one non-zero weight".to_owned());
        }
        if self.workload.mix.cross_shard_txn > 0 && self.topology.ranges < 2 {
            return invalid("cross_shard_txn requires at least 2 ranges".to_owned());
        }
        if self.workload.mix.contended_update > 0 && self.workload.hot_rows == 0 {
            return invalid("contended_update requires hot_rows >= 1".to_owned());
        }
        for event in &self.faults {
            self.validate_fault(event)?;
        }
        Ok(())
    }

    fn validate_fault(&self, event: &FaultEvent) -> Result<(), ScenarioError> {
        let window = self.workload.duration;
        if event.at >= window {
            return Err(ScenarioError::Invalid(format!(
                "fault at t={} starts at or after the {} measurement window ends",
                event.at.human(),
                window.human()
            )));
        }
        let end = match &event.action {
            FaultAction::Partition { duration, .. }
            | FaultAction::Latency { duration, .. }
            | FaultAction::Throttle { duration, .. }
            | FaultAction::Flap { duration, .. } => event.at + *duration,
            FaultAction::KillNode { restart_after, .. } => {
                event.at + restart_after.unwrap_or(Time::ZERO)
            }
        };
        if end > window {
            return Err(ScenarioError::Invalid(format!(
                "fault at t={} runs until t={}, past the {} measurement window",
                event.at.human(),
                end.human(),
                window.human()
            )));
        }
        let check_target = |target: &FaultTarget| {
            let (label, index, bound) = match target {
                FaultTarget::Range(range) => ("range", *range, self.topology.ranges),
                FaultTarget::Sql(node) => ("node", *node, self.topology.nodes),
                FaultTarget::AllRanges | FaultTarget::AllSql => return Ok(()),
            };
            if index >= bound {
                return Err(ScenarioError::Invalid(format!(
                    "fault at t={} targets {label} {index} but topology has {bound}",
                    event.at.human()
                )));
            }
            Ok(())
        };
        match &event.action {
            FaultAction::Partition { target, .. }
            | FaultAction::Latency { target, .. }
            | FaultAction::Throttle { target, .. }
            | FaultAction::Flap { target, .. } => check_target(target),
            FaultAction::KillNode { node, .. } => {
                if *node >= self.topology.nodes {
                    return Err(ScenarioError::Invalid(format!(
                        "fault at t={} kills node {node} but topology has {} nodes",
                        event.at.human(),
                        self.topology.nodes
                    )));
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn minimal_yaml() -> &'static str {
        "
name: minimal
topology: { nodes: 2, ranges: 2 }
workload:
  connections: 4
  duration: 10s
  mix: { single_shard_insert: 1 }
"
    }

    #[test]
    fn minimal_scenario_parses_with_defaults() {
        let scenario: Scenario = serde_yaml::from_str(minimal_yaml()).expect("parse");
        scenario.validate().expect("valid");
        let expected = Scenario {
            name: "minimal".to_owned(),
            description: String::new(),
            topology: TopologySpec {
                nodes: 2,
                ranges: 2,
                clock_skew: BTreeMap::new(),
                cpus_per_node: None,
                broker_cpus: None,
            },
            mode: ModeSpec::LogicalTso,
            workload: WorkloadSpec {
                connections: 4,
                rate: RateSpec::Saturate,
                warmup: secs(5),
                duration: secs(10),
                mix: MixSpec {
                    single_shard_insert: 1,
                    cross_shard_txn: 0,
                    read_only: 0,
                    contended_update: 0,
                },
                hot_rows: 1000,
                zipf_exponent: 1.1,
            },
            faults: Vec::new(),
        };
        assert!(scenario == expected);
    }

    #[test]
    fn full_scenario_round_trips() {
        let yaml = "
name: full
description: everything at once
topology:
  nodes: 3
  ranges: 4
  clock_skew: { 0: 400ms, 2: -100ms }
mode:
  hlc: { max_offset: 300ms }
workload:
  connections: 32
  rate:
    fixed: { target_rate: 5000/s }
  warmup: 10s
  duration: 60s
  mix:
    single_shard_insert: 60
    cross_shard_txn: 20
    read_only: 15
    contended_update: 5
  hot_rows: 100
  zipf_exponent: 1.5
faults:
  - at: 20s
    partition: { target: 'range:0', duration: 10s }
  - at: 35s
    latency: { target: all-ranges, delay: 100ms, jitter: 20ms, duration: 15s }
  - at: 40s
    throttle: { target: 'sql:1', rate: 64KiB/s, duration: 5s }
  - at: 50s
    kill_node: { node: 2, restart_after: 5s }
  - at: 50s
    flap: { target: 'range:1', period: 2s, duration: 8s }
";
        let scenario: Scenario = serde_yaml::from_str(yaml).expect("parse");
        scenario.validate().expect("valid");
        assert!(
            scenario.mode
                == ModeSpec::Hlc {
                    max_offset: millis(300)
                }
        );
        assert!(
            scenario.workload.rate
                == RateSpec::Fixed {
                    target_rate: per_sec(5000)
                }
        );
        assert!(
            scenario.topology.clock_skew == BTreeMap::from([(0, millis(400)), (2, -millis(100))])
        );
        assert!(scenario.faults.len() == 5);
        let round_tripped: Scenario =
            serde_yaml::from_str(&serde_yaml::to_string(&scenario).expect("serialize"))
                .expect("re-parse");
        assert!(round_tripped == scenario);
    }

    #[test]
    fn quantity_fields_reject_unitless_numbers() {
        // Every dimensioned scenario field must carry its unit: `30` is not
        // a duration, and guessing between seconds and milliseconds is the
        // failure the quantity types exist to prevent. Each case is the
        // whole document with exactly one field stripped of its unit.
        let cases: [(&str, &str); 6] = [
            (
                "workload.duration",
                "
name: unitless
topology: { nodes: 1, ranges: 1 }
workload: { connections: 4, duration: 10, mix: { single_shard_insert: 1 } }
",
            ),
            (
                "workload.warmup",
                "
name: unitless
topology: { nodes: 1, ranges: 1 }
workload:
  connections: 4
  warmup: 5
  duration: 10s
  mix: { single_shard_insert: 1 }
",
            ),
            (
                "workload.rate.fixed.target_rate",
                "
name: unitless
topology: { nodes: 1, ranges: 1 }
workload:
  connections: 4
  rate:
    fixed: { target_rate: 500 }
  duration: 10s
  mix: { single_shard_insert: 1 }
",
            ),
            (
                "topology.clock_skew",
                "
name: unitless
topology: { nodes: 1, ranges: 1, clock_skew: { 0: 400 } }
mode:
  hlc: { max_offset: 250ms }
workload: { connections: 4, duration: 10s, mix: { single_shard_insert: 1 } }
",
            ),
            (
                "faults[].at",
                "
name: unitless
topology: { nodes: 1, ranges: 1 }
workload: { connections: 4, duration: 10s, mix: { single_shard_insert: 1 } }
faults:
  - at: 1
    partition: { target: 'range:0', duration: 2s }
",
            ),
            (
                "faults[].throttle.rate",
                "
name: unitless
topology: { nodes: 1, ranges: 1 }
workload: { connections: 4, duration: 10s, mix: { single_shard_insert: 1 } }
faults:
  - at: 1s
    throttle: { target: 'range:0', rate: 65536, duration: 2s }
",
            ),
        ];
        for (field, yaml) in cases {
            let parsed = serde_yaml::from_str::<Scenario>(yaml);
            assert!(
                parsed.is_err(),
                "{field} must reject a unitless value, got {parsed:?}"
            );
        }
    }

    #[test]
    fn fault_target_parses_and_displays() {
        let cases = [
            ("range:0", FaultTarget::Range(0)),
            ("range:12", FaultTarget::Range(12)),
            ("all-ranges", FaultTarget::AllRanges),
            ("sql:3", FaultTarget::Sql(3)),
            ("all-sql", FaultTarget::AllSql),
        ];
        for (input, expected) in cases {
            let parsed: FaultTarget = input.parse().expect("parse");
            assert!(parsed == expected, "input {input}");
            assert!(parsed.to_string() == input);
        }
        assert!(let Err(_) = "range:x".parse::<FaultTarget>());
        assert!(let Err(_) = "banana".parse::<FaultTarget>());
        assert!(let Err(_) = "sql:".parse::<FaultTarget>());
    }

    #[test]
    fn mode_displays_its_uncertainty_window_in_human_form() {
        check!(ModeSpec::LogicalTso.to_string() == "logical-tso");
        check!(
            ModeSpec::Hlc {
                max_offset: millis(250)
            }
            .to_string()
                == "hlc(max_offset=250ms)"
        );
    }

    #[test]
    fn validation_rejects_inconsistent_scenarios() {
        let broken = [
            (
                "topology: { nodes: 0, ranges: 1 }",
                "nodes must be at least 1",
            ),
            (
                "topology: { nodes: 1, ranges: 0 }",
                "ranges must be at least 1",
            ),
            (
                "topology: { nodes: 3, ranges: 2 }",
                "must not exceed topology.ranges",
            ),
            (
                "topology: { nodes: 1, ranges: 1, clock_skew: { 5: 100ms } }",
                "clock_skew references node 5",
            ),
            (
                "topology: { nodes: 1, ranges: 1, clock_skew: { 0: 100ms } }",
                "clock_skew requires hlc mode",
            ),
        ];
        for (topology, expected_fragment) in broken {
            let yaml = format!(
                "
name: broken
{topology}
workload:
  connections: 4
  duration: 10s
  mix: {{ single_shard_insert: 1 }}
"
            );
            let scenario: Scenario = serde_yaml::from_str(&yaml).expect("parse");
            assert!(let Err(ScenarioError::Invalid(message)) = scenario.validate());
            assert!(
                message.contains(expected_fragment),
                "expected {expected_fragment:?} in {message:?}"
            );
        }
    }

    #[test]
    fn validation_rejects_out_of_bounds_faults_and_cross_shard_on_one_range() {
        let yaml = "
name: bad-fault
topology: { nodes: 2, ranges: 2 }
workload:
  connections: 4
  duration: 10s
  mix: { single_shard_insert: 1 }
faults:
  - at: 1s
    kill_node: { node: 7 }
";
        let scenario: Scenario = serde_yaml::from_str(yaml).expect("parse");
        assert!(let Err(ScenarioError::Invalid(message)) = scenario.validate());
        assert!(message.contains("kills node 7"));

        // Faults must fit inside the measurement window: application after
        // the window ends, and timed faults (or restarts) running past it,
        // are both rejected; ending exactly at the window bound is allowed.
        let bounds_cases = [
            (
                "partition: { target: 'range:0', duration: 3s }",
                "7s",
                false,
            ),
            ("partition: { target: 'range:0', duration: 5s }", "6s", true),
            (
                "latency: { target: all-ranges, delay: 50ms, duration: 4s }",
                "7s",
                true,
            ),
            ("kill_node: { node: 1, restart_after: 8s }", "3s", true),
            ("kill_node: { node: 1 }", "9s", false),
            (
                "flap: { target: 'range:1', period: 2s, duration: 4s }",
                "6s",
                false,
            ),
        ];
        for (action, at, rejected) in bounds_cases {
            let yaml = format!(
                "
name: bounds
topology: {{ nodes: 2, ranges: 2 }}
workload:
  connections: 4
  duration: 10s
  mix: {{ single_shard_insert: 1 }}
faults:
  - at: {at}
    {action}
"
            );
            let scenario: Scenario = serde_yaml::from_str(&yaml).expect("parse");
            let result = scenario.validate();
            assert!(
                result.is_err() == rejected,
                "action {action} at t={at}: {result:?}"
            );
        }
        let yaml = "
name: late-start
topology: { nodes: 2, ranges: 2 }
workload:
  connections: 4
  duration: 10s
  mix: { single_shard_insert: 1 }
faults:
  - at: 10s
    partition: { target: 'range:0', duration: 1s }
";
        let scenario: Scenario = serde_yaml::from_str(yaml).expect("parse");
        assert!(let Err(ScenarioError::Invalid(message)) = scenario.validate());
        assert!(message.contains("starts at or after"));

        let yaml = "
name: cross-shard-one-range
topology: { nodes: 1, ranges: 1 }
workload:
  connections: 4
  duration: 10s
  mix: { cross_shard_txn: 1 }
";
        let scenario: Scenario = serde_yaml::from_str(yaml).expect("parse");
        assert!(let Err(ScenarioError::Invalid(message)) = scenario.validate());
        assert!(message.contains("cross_shard_txn requires at least 2 ranges"));
    }

    #[test]
    fn bundled_scenarios_parse_and_validate() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios");
        let mut seen = 0;
        for entry in std::fs::read_dir(&dir).expect("scenarios dir") {
            let path = entry.expect("dir entry").path();
            if path
                .extension()
                .is_some_and(|extension| extension == "yaml")
            {
                Scenario::from_yaml_file(&path)
                    .unwrap_or_else(|error| panic!("scenario {}: {error}", path.display()));
                seen += 1;
            }
        }
        assert!(seen >= 6, "expected bundled scenarios, found {seen}");
    }
}
