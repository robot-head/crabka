//! The YAML scenario schema: topology, timestamp mode, workload, fault timeline.
//!
//! A scenario is the single input to a harness run. [`Scenario::from_yaml_file`]
//! parses and validates it; every other module consumes the parsed types.

use std::{collections::BTreeMap, fmt, path::Path, str::FromStr};

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
    /// Per-node HLC wall-clock skew in milliseconds (node index → signed
    /// offset), passed to `--hlc-wall-offset-ms`. Only meaningful in HLC
    /// mode; today only the skew of the node hosting range 0 (the single
    /// HLC authority) is observable.
    #[serde(default)]
    pub clock_skew_ms: BTreeMap<u16, i64>,
}

/// Timestamp-source mode for the tenant under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ModeSpec {
    /// Centralized Percolator-style logical timestamp oracle on range 0.
    #[default]
    LogicalTso,
    /// Hybrid logical clock.
    Hlc {
        /// Uncertainty window (`max_offset`) in milliseconds.
        #[serde(default = "default_hlc_max_offset_ms")]
        max_offset_ms: u64,
    },
}

fn default_hlc_max_offset_ms() -> u64 {
    250
}

impl fmt::Display for ModeSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogicalTso => write!(f, "logical-tso"),
            Self::Hlc { max_offset_ms } => write!(f, "hlc(max_offset_ms={max_offset_ms})"),
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
    /// Seconds of load before measurement starts.
    #[serde(default = "default_warmup_s")]
    pub warmup_s: u64,
    /// Seconds of measured load.
    pub duration_s: u64,
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

fn default_warmup_s() -> u64 {
    5
}

fn default_hot_rows() -> u32 {
    1000
}

fn default_zipf_exponent() -> f64 {
    1.1
}

/// Pacing for the workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum RateSpec {
    /// Issue transactions as fast as the cluster accepts them.
    #[default]
    Saturate,
    /// Target a fixed aggregate transaction rate across all connections.
    Fixed {
        /// Transactions per second across the whole workload.
        tps: u32,
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
    /// Seconds after the start of the measurement window.
    pub at_s: u64,
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
        /// Seconds until the link heals.
        duration_s: u64,
        /// How the cut manifests to peers.
        #[serde(default)]
        style: PartitionStyle,
    },
    /// Add one-way delay to a link for a duration.
    Latency {
        /// Which endpoint's proxy to affect.
        target: FaultTarget,
        /// Base one-way delay in milliseconds, applied in each direction.
        ms: u64,
        /// Uniform jitter added on top of the base delay.
        #[serde(default)]
        jitter_ms: u64,
        /// Seconds until the delay is removed.
        duration_s: u64,
    },
    /// Cap a link's bandwidth for a duration.
    Throttle {
        /// Which endpoint's proxy to affect.
        target: FaultTarget,
        /// Bandwidth cap in bytes per second, per direction.
        bytes_per_sec: u64,
        /// Seconds until the cap is removed.
        duration_s: u64,
    },
    /// SIGKILL a node's process, optionally restarting it later.
    KillNode {
        /// Node index to kill.
        node: u16,
        /// Seconds to wait before restarting the node. `None` leaves it down.
        #[serde(default)]
        restart_after_s: Option<u64>,
    },
    /// Repeatedly partition and heal a link.
    Flap {
        /// Which endpoint's proxy to affect.
        target: FaultTarget,
        /// Seconds per on/off half-cycle.
        period_s: u64,
        /// Total seconds the link keeps flapping (ends healed).
        duration_s: u64,
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
            .clock_skew_ms
            .keys()
            .find(|node| **node >= self.topology.nodes)
        {
            return invalid(format!(
                "clock_skew_ms references node {node} but topology has {} nodes",
                self.topology.nodes
            ));
        }
        if !self.topology.clock_skew_ms.is_empty() && self.mode == ModeSpec::LogicalTso {
            return invalid("clock_skew_ms requires hlc mode".to_owned());
        }
        if self.workload.connections == 0 {
            return invalid("workload.connections must be at least 1".to_owned());
        }
        if self.workload.duration_s == 0 {
            return invalid("workload.duration_s must be at least 1".to_owned());
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
        let check_target = |target: &FaultTarget| {
            let (label, index, bound) = match target {
                FaultTarget::Range(range) => ("range", *range, self.topology.ranges),
                FaultTarget::Sql(node) => ("node", *node, self.topology.nodes),
                FaultTarget::AllRanges | FaultTarget::AllSql => return Ok(()),
            };
            if index >= bound {
                return Err(ScenarioError::Invalid(format!(
                    "fault at t={}s targets {label} {index} but topology has {bound}",
                    event.at_s
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
                        "fault at t={}s kills node {node} but topology has {} nodes",
                        event.at_s, self.topology.nodes
                    )));
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn minimal_yaml() -> &'static str {
        "
name: minimal
topology: { nodes: 2, ranges: 2 }
workload:
  connections: 4
  duration_s: 10
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
                clock_skew_ms: BTreeMap::new(),
            },
            mode: ModeSpec::LogicalTso,
            workload: WorkloadSpec {
                connections: 4,
                rate: RateSpec::Saturate,
                warmup_s: 5,
                duration_s: 10,
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
  clock_skew_ms: { 0: 400, 2: -100 }
mode:
  hlc: { max_offset_ms: 300 }
workload:
  connections: 32
  rate:
    fixed: { tps: 5000 }
  warmup_s: 10
  duration_s: 60
  mix:
    single_shard_insert: 60
    cross_shard_txn: 20
    read_only: 15
    contended_update: 5
  hot_rows: 100
  zipf_exponent: 1.5
faults:
  - at_s: 20
    partition: { target: 'range:0', duration_s: 10 }
  - at_s: 35
    latency: { target: all-ranges, ms: 100, jitter_ms: 20, duration_s: 15 }
  - at_s: 40
    throttle: { target: 'sql:1', bytes_per_sec: 65536, duration_s: 5 }
  - at_s: 50
    kill_node: { node: 2, restart_after_s: 5 }
  - at_s: 55
    flap: { target: 'range:1', period_s: 2, duration_s: 8 }
";
        let scenario: Scenario = serde_yaml::from_str(yaml).expect("parse");
        scenario.validate().expect("valid");
        assert!(scenario.mode == ModeSpec::Hlc { max_offset_ms: 300 });
        assert!(scenario.workload.rate == RateSpec::Fixed { tps: 5000 });
        assert!(scenario.faults.len() == 5);
        let round_tripped: Scenario =
            serde_yaml::from_str(&serde_yaml::to_string(&scenario).expect("serialize"))
                .expect("re-parse");
        assert!(round_tripped == scenario);
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
                "topology: { nodes: 1, ranges: 1, clock_skew_ms: { 5: 100 } }",
                "clock_skew_ms references node 5",
            ),
            (
                "topology: { nodes: 1, ranges: 1, clock_skew_ms: { 0: 100 } }",
                "clock_skew_ms requires hlc mode",
            ),
        ];
        for (topology, expected_fragment) in broken {
            let yaml = format!(
                "
name: broken
{topology}
workload:
  connections: 4
  duration_s: 10
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
  duration_s: 10
  mix: { single_shard_insert: 1 }
faults:
  - at_s: 1
    kill_node: { node: 7 }
";
        let scenario: Scenario = serde_yaml::from_str(yaml).expect("parse");
        assert!(let Err(ScenarioError::Invalid(message)) = scenario.validate());
        assert!(message.contains("kills node 7"));

        let yaml = "
name: cross-shard-one-range
topology: { nodes: 1, ranges: 1 }
workload:
  connections: 4
  duration_s: 10
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
