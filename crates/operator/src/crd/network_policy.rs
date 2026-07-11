//! `Kafka.spec.networkPolicy` — operator-side surface for
//! generating a cluster-level `networking.k8s.io/v1.NetworkPolicy`.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Cluster-level opt-in for operator-managed `NetworkPolicy` generation.
/// Setting `Kafka.spec.networkPolicy` (including `{}`) enables generation;
/// `None` disables and triggers a one-shot orphan cleanup gated on the
/// previous `NetworkPolicyReady=Available` condition.
///
/// The struct intentionally carries no fields today — future work can
/// add `metrics_peers`, `controller_peers`, etc. without a breaking schema
/// change.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicySpec {}

/// Subset of `networking.k8s.io/v1.NetworkPolicyPeer`. `ipBlock` is
/// intentionally omitted; it can be added later if external CIDR
/// allow-lists become a real need.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyPeer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_selector: Option<LabelSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn network_policy_spec_empty_round_trips() {
        let cfg: NetworkPolicySpec = serde_json::from_str("{}").unwrap();
        let back = serde_json::to_string(&cfg).unwrap();
        assert!(back == "{}");
    }

    #[test]
    fn peer_with_both_selectors_round_trips() {
        let json = r#"{
            "podSelector":{"matchLabels":{"role":"frontend"}},
            "namespaceSelector":{"matchLabels":{"team":"platform"}}
        }"#;
        let p: NetworkPolicyPeer = serde_json::from_str(json).unwrap();
        let pod = p.pod_selector.as_ref().expect("podSelector present");
        let ns = p
            .namespace_selector
            .as_ref()
            .expect("namespaceSelector present");
        assert!(
            pod.match_labels
                .as_ref()
                .and_then(|m| m.get("role"))
                .map(String::as_str)
                == Some("frontend")
        );
        assert!(
            ns.match_labels
                .as_ref()
                .and_then(|m| m.get("team"))
                .map(String::as_str)
                == Some("platform")
        );
        let back = serde_json::to_string(&p).unwrap();
        assert!(back.contains("\"podSelector\""), "got: {back}");
        assert!(back.contains("\"namespaceSelector\""), "got: {back}");
    }

    #[test]
    fn peer_omits_unset_selectors() {
        let p = NetworkPolicyPeer::default();
        let json = serde_json::to_string(&p).unwrap();
        assert!(json == "{}", "default peer must serialize to empty object");
    }
}
