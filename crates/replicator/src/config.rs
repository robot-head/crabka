//! Replicator configuration model (clusters / flows / policies).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::ReplicatorError;

/// Top-level replicator configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatorConfig {
    /// Named cluster definitions keyed by alias.
    pub clusters: BTreeMap<String, ClusterConfig>,
    /// Directional replication flows.
    pub flows: Vec<FlowConfig>,
    /// Residency / compliance policies.
    #[serde(default)]
    pub policies: Vec<PolicyConfig>,
}

/// Configuration for a single Kafka cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Bootstrap server address (host:port).
    pub bootstrap: String,
    /// Logical region name (e.g. `"us"`, `"eu"`).
    pub region: String,
    /// Availability / compliance zones this cluster belongs to.
    #[serde(default)]
    pub zones: Vec<String>,
}

/// A directional replication flow from one cluster to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowConfig {
    /// Source cluster alias.
    pub from: String,
    /// Target cluster alias.
    pub to: String,
    /// Topic selector (include / exclude glob patterns).
    #[serde(default)]
    pub topics: Selectors,
    /// Consumer-group selector (include / exclude glob patterns).
    #[serde(default)]
    pub groups: Selectors,
    /// How replicated topics are named on the target cluster.
    #[serde(default)]
    pub naming: NamingPolicy,
    /// Delivery guarantee.
    #[serde(default)]
    pub delivery: Delivery,
}

/// Include / exclude glob-pattern selector lists.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Selectors {
    /// Patterns that must match.
    #[serde(default)]
    pub include: Vec<String>,
    /// Patterns that cause a match to be skipped.
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// How replicated topic names are derived on the target cluster.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NamingPolicy {
    /// Prefix topic name with the source cluster alias (`MirrorMaker` 2 default).
    #[default]
    Default,
    /// Keep the original topic name unchanged.
    Identity,
}

/// Delivery guarantee for a flow.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Delivery {
    /// At-least-once delivery (supported from Slice 1).
    #[default]
    AtLeastOnce,
    /// Exactly-once delivery (planned for Slice 3; rejected by Slice 1 validation).
    ExactlyOnce,
}

/// A residency / compliance policy that constrains where topic data may land.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// Human-readable policy name.
    pub name: String,
    /// Topics this policy applies to.
    #[serde(default)]
    pub topics: Vec<String>,
    /// Zone residency constraints.
    #[serde(default)]
    pub residency: Option<Residency>,
}

/// Zone-based data-residency constraints.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Residency {
    /// Zones to which data is permitted to flow.
    #[serde(default)]
    pub allow_zones: Vec<String>,
    /// Zones to which data must not flow.
    #[serde(default)]
    pub deny_zones: Vec<String>,
}

impl ReplicatorConfig {
    /// Parse a [`ReplicatorConfig`] from a YAML string.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicatorError::Config`] if the YAML is malformed or fails
    /// to deserialize.
    pub fn from_yaml(s: &str) -> Result<Self, ReplicatorError> {
        serde_yaml::from_str(s).map_err(|e| ReplicatorError::Config(e.to_string()))
    }

    /// Validate a parsed config against Slice-1 constraints.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicatorError::Config`] if:
    /// - A flow references an unknown cluster alias.
    /// - A flow requests `exactly-once` delivery (not supported until Slice 3).
    pub fn validate(&self) -> Result<(), ReplicatorError> {
        for f in &self.flows {
            if !self.clusters.contains_key(&f.from) {
                return Err(ReplicatorError::Config(format!(
                    "flow.from unknown cluster `{}`",
                    f.from
                )));
            }
            if !self.clusters.contains_key(&f.to) {
                return Err(ReplicatorError::Config(format!(
                    "flow.to unknown cluster `{}`",
                    f.to
                )));
            }
            if f.delivery == Delivery::ExactlyOnce {
                return Err(ReplicatorError::Config(
                    "delivery `exactly-once` is not supported until Slice 3; use `at-least-once`"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const YAML: &str = r#"
clusters:
  us-east: { bootstrap: "127.0.0.1:9092", region: us, zones: [us] }
  eu-west: { bootstrap: "127.0.0.1:9093", region: eu, zones: [eu, gdpr] }
flows:
  - from: us-east
    to: eu-west
    topics: { include: ["orders", "telemetry.*"], exclude: ["*.internal"] }
    groups: { include: ["analytics-*"] }
    naming: default
    delivery: at-least-once
policies:
  - name: keep-pii-in-eu
    topics: ["customers"]
    residency: { allow_zones: [gdpr] }
"#;

    #[test]
    fn parses_and_validates() {
        let cfg = ReplicatorConfig::from_yaml(YAML).unwrap();
        assert_eq!(
            (
                cfg.clusters.len(),
                cfg.clusters["eu-west"].zones.clone(),
                cfg.flows[0].naming,
                cfg.flows[0].delivery
            ),
            (
                2,
                vec!["eu".to_string(), "gdpr".to_string()],
                NamingPolicy::Default,
                Delivery::AtLeastOnce
            )
        );
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_eos_in_slice1() {
        let y = YAML.replace("delivery: at-least-once", "delivery: exactly-once");
        let cfg = ReplicatorConfig::from_yaml(&y).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(format!("{err}").contains("exactly-once"), "got: {err}");
    }

    #[test]
    fn rejects_unknown_cluster_ref() {
        let y = YAML.replace("to: eu-west", "to: nowhere");
        let cfg = ReplicatorConfig::from_yaml(&y).unwrap();
        assert!(cfg.validate().is_err());
    }
}
