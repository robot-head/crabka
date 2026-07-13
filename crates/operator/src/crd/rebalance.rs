//! `KafkaRebalance` CRD. Strimzi-shaped. The operator
//! translates the spec into Connect-RPC calls against the standalone
//! `crabka-rebalancer` service and surfaces the proposal
//! lifecycle back through the CRD's `status` subresource.
//!
//! Workflow mirrors Strimzi's annotation-driven state machine: the
//! operator computes a proposal (state `ProposalReady`), the human (or
//! `GitOps`) approves it via the `crabka.io/rebalance: approve` annotation,
//! the operator drives execution (`Rebalancing`) and polls to completion
//! (`Ready` / `NotReady`). `refresh` recomputes; `stop` cancels a running
//! execution.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ids::{LeaderMovementCount, MaxLeadersCount, MaxReplicasCount, ReplicaMovementCount};

#[derive(CustomResource, Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "KafkaRebalance",
    plural = "kafkarebalances",
    singular = "kafkarebalance",
    shortname = "kr",
    namespaced,
    status = "KafkaRebalanceStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaRebalanceSpec {
    /// Optimization goals to apply, by name (e.g. `RackAware`,
    /// `ReplicaDistribution`). When omitted or empty the rebalancer uses
    /// its full default goal registry in priority order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goals: Option<Vec<String>>,

    /// Replication throttle (bytes/sec) applied while the proposal
    /// executes (KIP-73). When omitted the rebalancer falls back to its
    /// own `--default-throttle-bytes-per-sec`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub throttle_bytes_per_sec: Option<i64>,

    /// Connect-RPC base URL of the `crabka-rebalancer` service, e.g.
    /// `http://my-cluster-rebalancer.kafka.svc:9300`. When omitted the
    /// operator derives `http://<cluster>-rebalancer.<namespace>.svc.cluster.local:9300`
    /// from the `crabka.io/cluster` label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

/// Projection of the rebalancer's `ProposalSummary` onto the CRD status.
/// All fields default to `0` so a proposal with no movements still
/// produces a complete (zeroed) result block.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OptimizationResult {
    /// Number of partition replica reassignments in the proposal.
    #[serde(default)]
    pub replica_movements: ReplicaMovementCount,
    /// Number of leadership changes in the proposal.
    #[serde(default)]
    pub leader_movements: LeaderMovementCount,
    /// Max replicas on any one broker before the proposal applies.
    #[serde(default)]
    pub max_replicas_before: MaxReplicasCount,
    /// Max replicas on any one broker after the proposal applies.
    #[serde(default)]
    pub max_replicas_after: MaxReplicasCount,
    /// Max partitions led by any one broker before the proposal applies.
    #[serde(default)]
    pub max_leaders_before: MaxLeadersCount,
    /// Max partitions led by any one broker after the proposal applies.
    #[serde(default)]
    pub max_leaders_after: MaxLeadersCount,
    /// The goals the rebalancer actually applied (post-selection).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub goals: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaRebalanceStatus {
    /// Kubernetes-style condition list. The active condition's `type`
    /// carries the rebalance state: one of `PendingProposal`,
    /// `ProposalReady`, `Rebalancing`, `Ready`, `NotReady`, `Stopped`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last spec that produced the current
    /// proposal (advanced when a fresh proposal is computed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Rebalancer-assigned proposal id. Stored so `approve` / `stop` /
    /// poll operations target the same proposal across reconciles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,

    /// Summary of the most recently computed proposal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimization_result: Option<OptimizationResult>,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaRebalance::crd();
        check!(crd.spec.group == "crabka.io");
        check!(crd.spec.names.kind == "KafkaRebalance");
        check!(crd.spec.names.plural == "kafkarebalances");
        check!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"kr".to_string())),
            "expected shortname `kr`",
        );
        check!(crd.spec.versions.len() == 1);
        check!(crd.spec.versions[0].name == "v1alpha1");
    }

    #[test]
    fn spec_round_trips_through_json() {
        let kr = KafkaRebalance::new(
            "demo-rebalance",
            KafkaRebalanceSpec {
                goals: Some(vec!["RackAware".into(), "ReplicaDistribution".into()]),
                throttle_bytes_per_sec: Some(10_000_000),
                endpoint: Some("http://r.kafka.svc:9300".into()),
            },
        );
        let json = serde_json::to_string(&kr).unwrap();
        for want in [
            "\"goals\":[\"RackAware\"",
            "\"throttleBytesPerSec\":10000000",
            "\"endpoint\":\"http://r.kafka.svc:9300\"",
        ] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let back: KafkaRebalance = serde_json::from_str(&json).unwrap();
        assert!(back.spec == kr.spec);
    }

    #[test]
    fn empty_spec_parses_and_omits_optionals() {
        let spec: KafkaRebalanceSpec = serde_json::from_str("{}").unwrap();
        assert!(
            spec == KafkaRebalanceSpec {
                goals: None,
                throttle_bytes_per_sec: None,
                endpoint: None,
            }
        );
        let j = serde_json::to_string(&spec).unwrap();
        assert!(j == "{}", "all-default spec must serialize to empty object");
    }

    #[test]
    fn status_omits_optional_fields_when_none() {
        let status = KafkaRebalanceStatus {
            conditions: vec![],
            observed_generation: Some(3),
            session_id: None,
            optimization_result: None,
        };
        let j = serde_json::to_string(&status).unwrap();
        check!(!j.contains("sessionId"), "got: {j}");
        check!(!j.contains("optimizationResult"), "got: {j}");
        check!(j.contains("\"observedGeneration\":3"), "got: {j}");
    }

    #[test]
    fn optimization_result_defaults_to_zeroes() {
        let r: OptimizationResult = serde_json::from_str("{}").unwrap();
        assert!(r == OptimizationResult::default());
    }
}
