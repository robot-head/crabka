//! Replicator configuration model (clusters / flows / policies).

use std::{collections::BTreeMap, num::NonZeroUsize, str::FromStr};

use clap::Args;
use crabka_units::{Time, millis, secs};
use refined_type::rule::GreaterI16;
use serde::{Deserialize, Serialize};

use crate::error::ReplicatorError;

/// Kafka client resource policy owned by the replicator process.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClientResourcePolicy {
    pub dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    pub frame_max: crabka_client_core::ClientFrameMax,
}

type RefinedReplicationFactor = GreaterI16<0>;

/// Positive Kafka replication factor accepted by the replicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationFactor(i16);

impl Default for ReplicationFactor {
    fn default() -> Self {
        Self(1)
    }
}

impl ReplicationFactor {
    /// Validate a Kafka replication factor.
    ///
    /// # Errors
    /// Returns an error when the value is not positive.
    pub fn new(value: i16) -> Result<Self, String> {
        RefinedReplicationFactor::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("replication factor: {error}"))
    }

    /// Return the Kafka protocol value.
    #[must_use]
    pub const fn get(self) -> i16 {
        self.0
    }
}

impl FromStr for ReplicationFactor {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.parse::<i16>().map_err(|error| error.to_string())?)
    }
}

/// Process-owned replicator runtime and topic policy.
#[derive(Args, Debug, Clone, PartialEq)]
pub struct ReplicatorRuntimePolicy {
    #[arg(long, env = "CRABKA_REPLICATOR_TOPIC_CREATE_TIMEOUT", default_value = "10s", value_parser = crabka_units::parse::positive_time)]
    pub topic_create_timeout: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_SOURCE_POLL_TIMEOUT", default_value = "500ms", value_parser = crabka_units::parse::positive_time)]
    pub source_poll_timeout: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_INTERNAL_DRAIN_POLL_TIMEOUT", default_value = "500ms", value_parser = crabka_units::parse::positive_time)]
    pub internal_drain_poll_timeout: Time,
    #[arg(
        long,
        env = "CRABKA_REPLICATOR_INTERNAL_DRAIN_EMPTY_POLLS",
        default_value = "3"
    )]
    pub internal_drain_empty_polls: NonZeroUsize,
    #[arg(long, env = "CRABKA_REPLICATOR_WORKER_BUILD_RETRY_BUDGET", default_value = "30s", value_parser = crabka_units::parse::positive_time)]
    pub worker_build_retry_budget: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_WORKER_BUILD_INITIAL_BACKOFF", default_value = "250ms", value_parser = crabka_units::parse::positive_time)]
    pub worker_build_initial_backoff: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_WORKER_BUILD_MAX_BACKOFF", default_value = "8s", value_parser = crabka_units::parse::positive_time)]
    pub worker_build_max_backoff: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_CONNECT_COMMIT_INTERVAL", default_value = "500ms", value_parser = crabka_units::parse::positive_time)]
    pub connect_commit_interval: Time,
    #[arg(
        long,
        env = "CRABKA_REPLICATOR_CONNECT_MAX_BATCH_RECORDS",
        default_value = "500"
    )]
    pub connect_max_batch_records: NonZeroUsize,
    #[arg(long, env = "CRABKA_REPLICATOR_SUPERVISOR_INTERVAL", default_value = "3s", value_parser = crabka_units::parse::positive_time)]
    pub supervisor_interval: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_HEARTBEAT_INTERVAL", default_value = "1s", value_parser = crabka_units::parse::positive_time)]
    pub heartbeat_interval: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_CHECKPOINT_INTERVAL", default_value = "5s", value_parser = crabka_units::parse::positive_time)]
    pub checkpoint_interval: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_CLIENT_DNS_TIMEOUT", default_value = "10s", value_parser = crabka_units::parse::positive_time)]
    pub client_dns_timeout: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_CLIENT_CONNECT_TIMEOUT", default_value = "5s", value_parser = crabka_units::parse::positive_time)]
    pub client_connect_timeout: Time,
    #[arg(long, env = "CRABKA_REPLICATOR_CLIENT_REQUEST_TIMEOUT", default_value = "30s", value_parser = crabka_units::parse::positive_time)]
    pub client_request_timeout: Time,
    #[arg(
        long,
        env = "CRABKA_REPLICATOR_DATA_TOPIC_REPLICATION_FACTOR",
        default_value = "1"
    )]
    pub data_topic_replication_factor: ReplicationFactor,
    #[arg(
        long,
        env = "CRABKA_REPLICATOR_INTERNAL_TOPIC_REPLICATION_FACTOR",
        default_value = "1"
    )]
    pub internal_topic_replication_factor: ReplicationFactor,
}

impl ReplicatorRuntimePolicy {
    /// Validate relationships between independently parsed policy values.
    ///
    /// # Errors
    /// Returns an error when retry bounds are inconsistent or the DNS timeout
    /// cannot be represented by the client boundary.
    pub fn validate(&self) -> Result<(), String> {
        if self.worker_build_initial_backoff > self.worker_build_max_backoff {
            return Err("worker build initial backoff exceeds maximum".to_owned());
        }
        if self.worker_build_retry_budget < self.worker_build_initial_backoff {
            return Err("worker build retry budget is below initial backoff".to_owned());
        }
        crabka_client_core::ClientDnsTimeout::new(self.client_dns_timeout)?;
        Ok(())
    }
}

impl Default for ReplicatorRuntimePolicy {
    fn default() -> Self {
        Self {
            topic_create_timeout: secs(10),
            source_poll_timeout: millis(500),
            internal_drain_poll_timeout: millis(500),
            internal_drain_empty_polls: NonZeroUsize::new(3).expect("default is positive"),
            worker_build_retry_budget: secs(30),
            worker_build_initial_backoff: millis(250),
            worker_build_max_backoff: secs(8),
            connect_commit_interval: millis(500),
            connect_max_batch_records: NonZeroUsize::new(500).expect("default is positive"),
            supervisor_interval: secs(3),
            heartbeat_interval: secs(1),
            checkpoint_interval: secs(5),
            client_dns_timeout: secs(10),
            client_connect_timeout: secs(5),
            client_request_timeout: secs(30),
            data_topic_replication_factor: ReplicationFactor(1),
            internal_topic_replication_factor: ReplicationFactor(1),
        }
    }
}

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
    /// Exactly-once delivery, planned for Slice 3. Slice 1 validation rejects
    /// it.
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
    /// Zones that data may flow to.
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
        assert2::assert!(cfg.clusters.len() == 2);
        assert2::assert!(
            cfg.clusters["eu-west"].zones.clone() == vec!["eu".to_string(), "gdpr".to_string()]
        );
        assert2::assert!(cfg.flows[0].naming == NamingPolicy::Default);
        assert2::assert!(cfg.flows[0].delivery == Delivery::AtLeastOnce);
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_eos_in_slice1() {
        let y = YAML.replace("delivery: at-least-once", "delivery: exactly-once");
        let cfg = ReplicatorConfig::from_yaml(&y).unwrap();
        let err = cfg.validate().unwrap_err();
        assert2::assert!(format!("{err}").contains("exactly-once"));
    }

    #[test]
    fn rejects_unknown_cluster_ref() {
        let y = YAML.replace("to: eu-west", "to: nowhere");
        let cfg = ReplicatorConfig::from_yaml(&y).unwrap();
        assert2::assert!(cfg.validate().is_err());
    }

    #[test]
    fn runtime_policy_defaults_preserve_existing_behavior() {
        let policy = ReplicatorRuntimePolicy::default();
        assert2::assert!(policy.topic_create_timeout == secs(10));
        assert2::assert!(policy.source_poll_timeout == millis(500));
        assert2::assert!(policy.internal_drain_empty_polls.get() == 3);
        assert2::assert!(policy.worker_build_retry_budget == secs(30));
        assert2::assert!(policy.worker_build_initial_backoff == millis(250));
        assert2::assert!(policy.worker_build_max_backoff == secs(8));
        assert2::assert!(policy.connect_max_batch_records.get() == 500);
        assert2::assert!(policy.supervisor_interval == secs(3));
        assert2::assert!(policy.heartbeat_interval == secs(1));
        assert2::assert!(policy.checkpoint_interval == secs(5));
        assert2::assert!(policy.client_dns_timeout == secs(10));
        assert2::assert!(policy.data_topic_replication_factor.get() == 1);
        policy.validate().unwrap();
    }

    #[test]
    fn runtime_policy_rejects_invalid_relations_and_replication_factors() {
        let policy = ReplicatorRuntimePolicy {
            worker_build_initial_backoff: secs(9),
            ..ReplicatorRuntimePolicy::default()
        };
        assert2::assert!(policy.validate().is_err());

        let policy = ReplicatorRuntimePolicy {
            worker_build_retry_budget: millis(100),
            ..ReplicatorRuntimePolicy::default()
        };
        assert2::assert!(policy.validate().is_err());
        assert2::assert!("0".parse::<ReplicationFactor>().is_err());
        assert2::assert!("2".parse::<ReplicationFactor>().unwrap().get() == 2);
    }
}
