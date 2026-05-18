# Crabka Operator Slice 23 — `Kafka.spec.networkPolicy` (NetworkPolicy generation)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development`. Per CLAUDE.md, dispatch tasks within a batch in parallel; sequential between batches. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Surface an opt-in `Kafka.spec.networkPolicy` field that lets a cluster operator restrict ingress to broker/controller pods via a generated `networking.k8s.io/v1.NetworkPolicy`. Per-listener peer allow-lists live on the existing `Listener` struct. Operator auto-allows its own admin traffic and (when configured) the metrics scrape port.

**Spec:** [`docs/superpowers/specs/2026-05-17-crabka-operator-network-policy-23-design.md`](../specs/2026-05-17-crabka-operator-network-policy-23-design.md).

**Tech stack:** Rust 2024, `kube-rs`, `k8s-openapi` (`networking.k8s.io/v1.NetworkPolicy`), `schemars`, `serde_json`, Helm, kind, Calico CNI (e2e only).

---

## Batch overview

| Batch | Tasks | Files (disjoint within batch) | Parallel? |
|---|---|---|---|
| 1 | T1, T4 | `crd/network_policy.rs` + `crd/kafka.rs` + `crd/listener.rs` + `crd/mod.rs` ‖ `charts/.../clusterrole.yaml` | yes |
| 2 | T2 | `controller/network_policy.rs` + `controller/mod.rs` | — |
| 3 | T3 | `controller/kafka.rs` + `tests/reconcile_kafka.rs` | — |
| 4 | T5, T6 | `deploy/crds/crabka.io_kafkas.yaml` (regen) ‖ `.github/workflows/operator-e2e.yml` | yes |

Dependencies: T2 imports T1's CRD types. T3 imports T2's `reconcile_network_policy`. T5 regenerates from T1's types. T6 references T3's `NetworkPolicyReady` condition.

---

## Task 1 — CRD types: `NetworkPolicySpec`, `NetworkPolicyPeer`, and field additions

**Files:**
- Create: `crates/operator/src/crd/network_policy.rs`
- Modify: `crates/operator/src/crd/mod.rs`
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `crates/operator/src/crd/listener.rs`

- [ ] **Step 1: Create `crd/network_policy.rs`**

```rust
//! Slice 23: `Kafka.spec.networkPolicy` — operator-side surface for
//! generating a cluster-level `networking.k8s.io/v1.NetworkPolicy`.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Cluster-level opt-in for operator-managed NetworkPolicy generation.
/// Setting `Kafka.spec.networkPolicy` (including `{}`) enables generation;
/// `None` disables and triggers a one-shot orphan cleanup gated on the
/// previous `NetworkPolicyReady=Available` condition.
///
/// The struct intentionally carries no fields today — future slices can
/// add `metrics_peers`, `controller_peers`, etc. without a breaking schema
/// change.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicySpec {}

/// Subset of `networking.k8s.io/v1.NetworkPolicyPeer`. `ipBlock` is
/// intentionally omitted; a future slice can add it if external CIDR
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
    use super::*;

    #[test]
    fn network_policy_spec_empty_round_trips() {
        let cfg: NetworkPolicySpec = serde_json::from_str("{}").unwrap();
        let back = serde_json::to_string(&cfg).unwrap();
        assert_eq!(back, "{}");
    }

    #[test]
    fn peer_with_both_selectors_round_trips() {
        let json = r#"{
            "podSelector":{"matchLabels":{"role":"frontend"}},
            "namespaceSelector":{"matchLabels":{"team":"platform"}}
        }"#;
        let p: NetworkPolicyPeer = serde_json::from_str(json).unwrap();
        let pod = p.pod_selector.expect("podSelector present");
        let ns = p.namespace_selector.expect("namespaceSelector present");
        assert_eq!(
            pod.match_labels.as_ref().and_then(|m| m.get("role")).map(String::as_str),
            Some("frontend"),
        );
        assert_eq!(
            ns.match_labels.as_ref().and_then(|m| m.get("team")).map(String::as_str),
            Some("platform"),
        );
        let back = serde_json::to_string(&p).unwrap();
        assert!(back.contains("\"podSelector\""), "got: {back}");
        assert!(back.contains("\"namespaceSelector\""), "got: {back}");
    }

    #[test]
    fn peer_omits_unset_selectors() {
        let p = NetworkPolicyPeer::default();
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "{}", "default peer must serialize to empty object");
    }
}
```

- [ ] **Step 2: Add `pub mod network_policy;` + re-exports to `crd/mod.rs`**

Replace the existing `crd/mod.rs` contents with:

```rust
//! CRD type definitions. Each kind lives in its own submodule and is the
//! single source of truth for both the runtime types and the generated
//! CRD YAML manifest (see `gen_crds`).

pub mod kafka;
pub mod kafka_node_pool;
pub mod listener;
pub mod metrics;
pub mod network_policy;

pub use kafka::{Kafka, KafkaCondition, KafkaSpec, KafkaStatus};
pub use kafka_node_pool::{
    KafkaNodePool, KafkaNodePoolSpec, KafkaNodePoolStatus, MetadataTemplate, NodeRole,
    PersistentClaimSpec, PodTemplate, Storage,
};
pub use listener::*;
pub use metrics::{MetricsConfig, MetricsType, PodMonitorSpec, ServiceMonitorSpec};
pub use network_policy::{NetworkPolicyPeer, NetworkPolicySpec};
```

- [ ] **Step 3: Add `network_policy` field to `KafkaSpec`**

In `crd/kafka.rs`, insert this field at the end of the `KafkaSpec` struct (after `metrics_config`):

```rust
    /// Slice 23: opt-in NetworkPolicy generation. When `None`, no
    /// NetworkPolicy is generated. When `Some` (even `{}`), the operator
    /// renders a cluster-level NetworkPolicy gating ingress to broker /
    /// controller pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_policy: Option<crate::crd::NetworkPolicySpec>,
```

- [ ] **Step 4: Update every literal `KafkaSpec { … }` in `crd/kafka.rs` tests**

Each test in `crd/kafka.rs::tests` constructs a `KafkaSpec` literally. Append `network_policy: None,` after `metrics_config: None,` in each one (six call sites: `round_trips_through_json`, `spec_omits_metrics_config_when_none`, `spec_carries_metrics_config_pod_monitor`'s helper if any, plus the closure in `status_carries_listener_status` — find each `KafkaSpec {` literal and add the field).

The clean way to find them all:

Run: `grep -n "KafkaSpec {" crates/operator/src/crd/kafka.rs`

Expected: 4 matches in the test module. For each, add `network_policy: None,`.

- [ ] **Step 5: Add `KafkaSpec.network_policy` JSON tests**

In `crd/kafka.rs::tests`, add:

```rust
    #[test]
    fn spec_omits_network_policy_when_none() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
            },
        );
        let j = serde_json::to_string(&k.spec).unwrap();
        assert!(!j.contains("networkPolicy"), "got: {j}");
    }

    #[test]
    fn spec_carries_network_policy_when_set() {
        let json = r#"{"kafkaVersion":"0.1.1","networkPolicy":{}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.network_policy.is_some(), "networkPolicy parsed");
    }
```

- [ ] **Step 6: Add `network_policy_peers` field to `Listener`**

In `crd/listener.rs`, append this field after `configuration`:

```rust
    /// Slice 23: per-listener peer allow-list. Tri-state:
    /// - `None` → no per-listener restriction (allow-all on this port).
    /// - `Some(vec![])` → deny-all on this listener port (no per-listener
    ///   rule emitted; default-deny applies).
    /// - `Some(non_empty)` → only listed peers may reach this port.
    ///
    /// Only consulted when `Kafka.spec.networkPolicy` is set; otherwise
    /// inert. The operator auto-allow rule still fires on this port even
    /// for deny-all listeners so the operator can manage the cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_policy_peers: Option<Vec<crate::crd::NetworkPolicyPeer>>,
```

- [ ] **Step 7: Update every literal `Listener { … }` in `crd/listener.rs` tests**

Run: `grep -n "Listener {" crates/operator/src/crd/listener.rs`

Each construction needs `network_policy_peers: None,` appended (or `Some(vec![…])` for the new tests below).

- [ ] **Step 8: Add `Listener.network_policy_peers` JSON tests**

In `crd/listener.rs::tests`, add:

```rust
    #[test]
    fn listener_without_peers_omits_field() {
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            configuration: None,
            network_policy_peers: None,
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(!json.contains("networkPolicyPeers"), "got: {json}");
    }

    #[test]
    fn listener_with_empty_peers_round_trips() {
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            configuration: None,
            network_policy_peers: Some(vec![]),
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(json.contains("\"networkPolicyPeers\":[]"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn listener_with_named_peer_round_trips() {
        use crate::crd::NetworkPolicyPeer;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
        use std::collections::BTreeMap;

        let mut match_labels = BTreeMap::new();
        match_labels.insert("role".to_string(), "client".to_string());
        let peer = NetworkPolicyPeer {
            pod_selector: Some(LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            }),
            namespace_selector: None,
        };
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            configuration: None,
            network_policy_peers: Some(vec![peer]),
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(json.contains("\"networkPolicyPeers\""), "got: {json}");
        assert!(json.contains("\"matchLabels\":{\"role\":\"client\"}"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }
```

- [ ] **Step 9: Run the CRD-layer tests**

Run: `cargo test -p crabka-operator --lib crd::network_policy crd::kafka crd::listener`

Expected: every test in those modules passes (10+ tests).

- [ ] **Step 10: Run clippy on the operator crate**

Run: `cargo clippy -p crabka-operator --all-targets -- -D warnings`

Expected: clean.

- [ ] **Step 11: Commit**

```bash
git add crates/operator/src/crd/network_policy.rs \
        crates/operator/src/crd/mod.rs \
        crates/operator/src/crd/kafka.rs \
        crates/operator/src/crd/listener.rs
git commit -m "Slice 23 T1: NetworkPolicySpec + NetworkPolicyPeer CRD types"
```

---

## Task 2 — Renderer + reconcile in `controller/network_policy.rs`

**Depends on:** Task 1 (uses `crd::NetworkPolicyPeer` etc.)

**Files:**
- Create: `crates/operator/src/controller/network_policy.rs`
- Modify: `crates/operator/src/controller/mod.rs`

- [ ] **Step 1: Create `controller/network_policy.rs`**

```rust
//! Slice 23: NetworkPolicy reconcile — opt-in via `Kafka.spec.networkPolicy`.
//!
//! One `NetworkPolicy` per cluster, named `<cluster>-broker-policy`,
//! owner-ref'd to the parent `Kafka`. Selector targets every cluster pod
//! (broker / controller / combined) via `app.kubernetes.io/name=crabka-broker`
//! + `app.kubernetes.io/instance=<name>`.
//!
//! Ingress rules (stable order):
//!   1. Inter-broker pod-to-pod on the inter-broker listener port.
//!   2. Operator-auto-allow on every listener port (one rule each).
//!   3. Per-listener peer rules — tri-state on `Listener.networkPolicyPeers`
//!      (None=allow-all, Some([])=skip = deny-all, Some(peers)=restrict).
//!   4. Metrics port (9404) allow-all when `spec.metricsConfig` is set.
//!
//! Orphan cleanup: when `spec.networkPolicy` becomes `None` AND the cached
//! status has `NetworkPolicyReady=Available`, the operator DELETEs the
//! resource once. The next reconcile sees `Disabled` in status and stops
//! re-attempting the delete.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::{
    NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer as K8sPeer,
    NetworkPolicyPort, NetworkPolicySpec as K8sNpSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::Resource as _;
use kube::api::{Api, DeleteParams};

use crate::context::Context;
use crate::controller::common::{
    APP_LABEL, ReconcileError, apply_object, common_labels, owner_ref,
};
use crate::controller::kafka_node_pool::METRICS_PORT;
use crate::crd::{Kafka, Listener, NetworkPolicyPeer};

const OPERATOR_LABEL: &str = "crabka-operator";

/// Render the NetworkPolicy. Pure function of the Kafka CR + effective
/// listeners + inter-broker listener port + metrics-enabled bit.
pub(crate) fn render_network_policy(
    owner: &Kafka,
    effective_listeners: &[Listener],
    inter_broker_port: i32,
    metrics_enabled: bool,
) -> Result<NetworkPolicy, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let ns = owner.meta().namespace.clone();
    let labels = common_labels(&name, &owner.spec.kafka_version, None);

    // Pod selector: every cluster pod gets app.kubernetes.io/name=crabka-broker
    // and instance=<name>. A single selector covers all node-pool roles.
    let mut pod_match: BTreeMap<String, String> = BTreeMap::new();
    pod_match.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    pod_match.insert("app.kubernetes.io/instance".into(), name.clone());
    let pod_selector = LabelSelector {
        match_labels: Some(pod_match),
        match_expressions: None,
    };

    // Operator allow-rule peer (one peer used across all listener rules).
    let mut operator_match: BTreeMap<String, String> = BTreeMap::new();
    operator_match.insert("app.kubernetes.io/name".into(), OPERATOR_LABEL.into());
    let operator_peer = K8sPeer {
        pod_selector: Some(LabelSelector {
            match_labels: Some(operator_match),
            match_expressions: None,
        }),
        namespace_selector: None,
        ip_block: None,
    };

    // Self-selector peer for inter-broker traffic.
    let self_peer = K8sPeer {
        pod_selector: Some(pod_selector.clone()),
        namespace_selector: None,
        ip_block: None,
    };

    let mut ingress: Vec<NetworkPolicyIngressRule> = Vec::new();

    // 1. Inter-broker rule.
    ingress.push(NetworkPolicyIngressRule {
        from: Some(vec![self_peer]),
        ports: Some(vec![NetworkPolicyPort {
            protocol: Some("TCP".into()),
            port: Some(IntOrString::Int(inter_broker_port)),
            end_port: None,
        }]),
    });

    // 2. Operator allow-rule per listener port. Always emitted regardless
    //    of the per-listener peer tri-state so the operator never locks
    //    itself out.
    for l in effective_listeners {
        ingress.push(NetworkPolicyIngressRule {
            from: Some(vec![operator_peer.clone()]),
            ports: Some(vec![NetworkPolicyPort {
                protocol: Some("TCP".into()),
                port: Some(IntOrString::Int(l.port)),
                end_port: None,
            }]),
        });
    }

    // 3. Per-listener peer rules.
    //    None → allow-all (rule with empty `from`).
    //    Some([]) → skip (default-deny applies to that port).
    //    Some(peers) → convert to k8s peers + restrict.
    for l in effective_listeners {
        let rule_from = match l.network_policy_peers.as_deref() {
            None => Some(vec![]),
            Some([]) => continue,
            Some(peers) => Some(peers.iter().map(to_k8s_peer).collect()),
        };
        ingress.push(NetworkPolicyIngressRule {
            from: rule_from,
            ports: Some(vec![NetworkPolicyPort {
                protocol: Some("TCP".into()),
                port: Some(IntOrString::Int(l.port)),
                end_port: None,
            }]),
        });
    }

    // 4. Metrics-port rule (allow-all when metricsConfig is set).
    if metrics_enabled {
        ingress.push(NetworkPolicyIngressRule {
            from: Some(vec![]),
            ports: Some(vec![NetworkPolicyPort {
                protocol: Some("TCP".into()),
                port: Some(IntOrString::Int(METRICS_PORT)),
                end_port: None,
            }]),
        });
    }

    Ok(NetworkPolicy {
        metadata: ObjectMeta {
            name: Some(format!("{name}-broker-policy")),
            namespace: ns,
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(owner)?]),
            ..Default::default()
        },
        spec: Some(K8sNpSpec {
            pod_selector,
            policy_types: Some(vec!["Ingress".into()]),
            ingress: Some(ingress),
            egress: None,
        }),
    })
}

fn to_k8s_peer(p: &NetworkPolicyPeer) -> K8sPeer {
    K8sPeer {
        pod_selector: p.pod_selector.clone(),
        namespace_selector: p.namespace_selector.clone(),
        ip_block: None,
    }
}

/// Returns `None` when `spec.networkPolicy` is unset, `Some(Ok(()))` on
/// successful apply, `Some(Err(_))` on apply error.
///
/// Orphan cleanup: when `spec.networkPolicy` is unset AND
/// `status.conditions[NetworkPolicyReady].reason == "Available"`,
/// DELETEs the resource once (404-tolerant). On the next reconcile the
/// status will carry `reason=Disabled` so the delete won't repeat.
pub(crate) async fn reconcile_network_policy(
    ctx: &Context,
    owner: &Kafka,
    name: &str,
    namespace: &str,
    effective_listeners: &[Listener],
    inter_broker_port: i32,
) -> Option<Result<(), ReconcileError>> {
    let np_api: Api<NetworkPolicy> = Api::namespaced(ctx.client.clone(), namespace);

    if owner.spec.network_policy.is_none() {
        let was_rendered = owner.status.as_ref().is_some_and(|s| {
            s.conditions
                .iter()
                .any(|c| c.type_ == "NetworkPolicyReady" && c.reason == "Available")
        });
        if was_rendered {
            let _ = np_api
                .delete(&format!("{name}-broker-policy"), &DeleteParams::default())
                .await;
        }
        return None;
    }

    let metrics_enabled = owner.spec.metrics_config.is_some();
    let np = match render_network_policy(
        owner,
        effective_listeners,
        inter_broker_port,
        metrics_enabled,
    ) {
        Ok(np) => np,
        Err(e) => return Some(Err(e)),
    };
    let np_name = format!("{name}-broker-policy");
    if let Err(e) = apply_object(&np_api, &np_name, &np).await {
        return Some(Err(e));
    }
    Some(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::common::BROKER_PORT;
    use crate::crd::{KafkaSpec, ListenerType, NetworkPolicySpec};

    fn test_kafka() -> Kafka {
        let mut k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: Some(NetworkPolicySpec::default()),
            },
        );
        k.metadata.namespace = Some("default".into());
        k.metadata.uid = Some("00000000-0000-0000-0000-000000000001".into());
        k
    }

    fn internal_listener(name: &str, port: i32, peers: Option<Vec<NetworkPolicyPeer>>) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: false,
            configuration: None,
            network_policy_peers: peers,
        }
    }

    fn rules_targeting_port(np: &NetworkPolicy, port: i32) -> Vec<&NetworkPolicyIngressRule> {
        np.spec
            .as_ref()
            .and_then(|s| s.ingress.as_ref())
            .map(|rules| {
                rules
                    .iter()
                    .filter(|r| {
                        r.ports.as_ref().is_some_and(|ps| {
                            ps.iter().any(|p| {
                                p.port == Some(IntOrString::Int(port))
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn render_emits_inter_broker_rule() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let spec = np.spec.as_ref().unwrap();
        let inter = spec.ingress.as_ref().unwrap().first().unwrap();
        let from = inter.from.as_ref().unwrap();
        assert_eq!(from.len(), 1);
        let pod = from[0].pod_selector.as_ref().unwrap();
        let labels = pod.match_labels.as_ref().unwrap();
        assert_eq!(labels.get("app.kubernetes.io/name").map(String::as_str), Some(APP_LABEL));
        assert_eq!(labels.get("app.kubernetes.io/instance").map(String::as_str), Some("demo"));
    }

    #[test]
    fn render_emits_operator_allow_rule_per_listener() {
        let listeners = vec![
            internal_listener("PLAIN", 9092, None),
            internal_listener("EXTRA", 9094, None),
        ];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let spec = np.spec.as_ref().unwrap();
        let ingress = spec.ingress.as_ref().unwrap();

        // Operator-allow rules: one per listener. Detect by peer-label.
        let operator_rules: Vec<_> = ingress
            .iter()
            .filter(|r| {
                r.from.as_ref().is_some_and(|fs| {
                    fs.iter().any(|p| {
                        p.pod_selector.as_ref().is_some_and(|s| {
                            s.match_labels
                                .as_ref()
                                .and_then(|m| m.get("app.kubernetes.io/name"))
                                .map(String::as_str)
                                == Some(OPERATOR_LABEL)
                        })
                    })
                })
            })
            .collect();
        assert_eq!(operator_rules.len(), 2);
        let ports: Vec<i32> = operator_rules
            .iter()
            .map(|r| match &r.ports.as_ref().unwrap()[0].port {
                Some(IntOrString::Int(p)) => *p,
                _ => panic!("expected int port"),
            })
            .collect();
        assert!(ports.contains(&9092) && ports.contains(&9094), "ports={ports:?}");
    }

    #[test]
    fn render_unset_peers_listener_emits_allow_all() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        // Filter for the per-listener rule (not the operator-allow, not the inter-broker self_peer).
        let rules_on_9092 = rules_targeting_port(&np, 9092);
        // Expected rules on 9092: inter-broker self_peer, operator-allow, per-listener allow-all.
        // The per-listener allow-all has an empty `from`.
        let allow_all = rules_on_9092.iter().find(|r| {
            r.from.as_ref().is_some_and(|fs| fs.is_empty())
        });
        assert!(
            allow_all.is_some(),
            "expected an allow-all rule (empty `from`) on :9092"
        );
    }

    #[test]
    fn render_empty_peers_listener_skips_port_rule() {
        let listeners = vec![internal_listener("PLAIN", 9092, Some(vec![]))];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let rules_on_9092 = rules_targeting_port(&np, 9092);
        // Expected: inter-broker self_peer rule + operator-allow rule. No
        // per-listener rule (deny-all -> skipped).
        // The operator-allow has operator label; the inter-broker has the
        // pod selector with APP_LABEL. Count rules whose `from` is empty:
        // there should be zero (would indicate an allow-all rule slipped through).
        let allow_all = rules_on_9092.iter().find(|r| {
            r.from.as_ref().is_some_and(|fs| fs.is_empty())
        });
        assert!(
            allow_all.is_none(),
            "deny-all listener must not emit an allow-all (empty `from`) rule"
        );
        // Exactly two rules on 9092: inter-broker + operator-allow.
        assert_eq!(rules_on_9092.len(), 2);
    }

    #[test]
    fn render_non_empty_peers_listener_restricts() {
        let mut match_labels = BTreeMap::new();
        match_labels.insert("role".to_string(), "client".to_string());
        let peer = NetworkPolicyPeer {
            pod_selector: Some(LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            }),
            namespace_selector: None,
        };
        let listeners = vec![internal_listener("PLAIN", 9092, Some(vec![peer]))];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let rules_on_9092 = rules_targeting_port(&np, 9092);

        // Find the per-listener rule whose peer carries our custom label.
        let restricted = rules_on_9092.iter().find(|r| {
            r.from.as_ref().is_some_and(|fs| {
                fs.iter().any(|p| {
                    p.pod_selector.as_ref().is_some_and(|s| {
                        s.match_labels
                            .as_ref()
                            .and_then(|m| m.get("role"))
                            .map(String::as_str)
                            == Some("client")
                    })
                })
            })
        });
        assert!(restricted.is_some(), "expected per-listener restricted rule");
    }

    #[test]
    fn render_metrics_enabled_emits_metrics_port_rule() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, true).unwrap();
        let rules_on_9404 = rules_targeting_port(&np, METRICS_PORT);
        assert_eq!(rules_on_9404.len(), 1);
        // Allow-all on metrics (empty `from`).
        assert!(rules_on_9404[0].from.as_ref().is_some_and(|fs| fs.is_empty()));
    }

    #[test]
    fn render_metrics_disabled_no_metrics_port_rule() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let rules_on_9404 = rules_targeting_port(&np, METRICS_PORT);
        assert!(rules_on_9404.is_empty(), "no metrics rule when metricsConfig unset");
    }

    #[test]
    fn render_pod_selector_matches_pool_pods() {
        let listeners = vec![internal_listener("PLAIN", BROKER_PORT, None)];
        let np = render_network_policy(&test_kafka(), &listeners, BROKER_PORT, false).unwrap();
        let sel = np.spec.as_ref().unwrap().pod_selector.match_labels.as_ref().unwrap();
        assert_eq!(sel.get("app.kubernetes.io/name").map(String::as_str), Some(APP_LABEL));
        assert_eq!(sel.get("app.kubernetes.io/instance").map(String::as_str), Some("demo"));
    }

    #[test]
    fn render_policy_types_ingress_only() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let spec = np.spec.as_ref().unwrap();
        assert_eq!(spec.policy_types.as_ref().unwrap(), &vec!["Ingress".to_string()]);
        assert!(spec.egress.is_none());
    }

    #[test]
    fn render_name_and_namespace() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        assert_eq!(np.metadata.name.as_deref(), Some("demo-broker-policy"));
        assert_eq!(np.metadata.namespace.as_deref(), Some("default"));
    }

    #[test]
    fn render_owner_ref_set() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let refs = np.metadata.owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].kind, "Kafka");
        assert_eq!(refs[0].controller, Some(true));
    }
}
```

- [ ] **Step 2: Declare the module in `controller/mod.rs`**

Append `pub(crate) mod network_policy;` to `controller/mod.rs`. After the edit:

```rust
//! Controllers (reconcilers) for Crabka CRDs. Each kind lives in its own
//! submodule and shares helpers via `common` (cluster-level rendering,
//! SSA helpers, label / owner-ref builders, status derivation).

pub mod common;
pub mod kafka;
pub mod kafka_node_pool;
pub(crate) mod listeners;
pub(crate) mod metrics;
pub(crate) mod network_policy;
```

- [ ] **Step 3: Build + run the unit tests**

Run: `cargo test -p crabka-operator --lib controller::network_policy`

Expected: 11 tests pass.

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p crabka-operator --all-targets -- -D warnings`

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/operator/src/controller/network_policy.rs \
        crates/operator/src/controller/mod.rs
git commit -m "Slice 23 T2: render_network_policy + reconcile_network_policy"
```

---

## Task 3 — Wire reconcile_network_policy into `controller/kafka.rs` + reconcile tests

**Depends on:** Task 2.

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs`
- Modify: `crates/operator/tests/reconcile_kafka.rs`

- [ ] **Step 1: Import the module + add status-condition helper**

At the top of `controller/kafka.rs`, in the existing `use crate::controller::...` block, add:

```rust
use crate::controller::network_policy;
```

(Keep the rest of the imports as-is.)

- [ ] **Step 2: Add the NetworkPolicy reconcile + condition derivation**

In `kafka.rs::reconcile`, after the `metrics_condition` block and before building the final `status` value, insert:

```rust
    // Slice 23: NetworkPolicy reconcile (opt-in via spec.networkPolicy).
    // Inter-broker port: the listener whose name matches the effective
    // inter-broker name. Falls back to the synthesized default's BROKER_PORT
    // (defensive only; effective_listeners is always non-empty).
    let inter_broker_port = effective_listeners
        .iter()
        .find(|l| l.name == inter_broker_name)
        .map(|l| l.port)
        .unwrap_or(common::BROKER_PORT);

    let np_outcome = network_policy::reconcile_network_policy(
        &ctx,
        &obj,
        &name,
        &ns,
        &effective_listeners,
        inter_broker_port,
    )
    .await;
    let np_condition = match &np_outcome {
        None => condition(
            "NetworkPolicyReady",
            "False",
            "Disabled",
            "spec.networkPolicy is not set",
        ),
        Some(Ok(())) => condition(
            "NetworkPolicyReady",
            "True",
            "Available",
            "network policy reconciled",
        ),
        Some(Err(_)) => condition(
            "NetworkPolicyReady",
            "False",
            "Error",
            "network policy reconcile failed",
        ),
    };
```

- [ ] **Step 3: Append `np_condition` to the status conditions vector**

Locate the existing `let status = KafkaStatus { conditions: vec![…], … };` block. Add `np_condition,` after `metrics_condition,`:

```rust
    let status = KafkaStatus {
        conditions: vec![
            condition(
                "Ready",
                if ready { "True" } else { "False" },
                reason,
                &message,
            ),
            condition(
                "Rolling",
                if rolling { "True" } else { "False" },
                rolling_reason,
                &rolling_message,
            ),
            listeners_valid_cond,
            listeners_ready_cond,
            metrics_condition,
            np_condition,
        ],
        replicas: Some(rollup.replicas),
        ready_replicas: Some(rollup.ready_replicas),
        listeners: listener_status,
    };
```

- [ ] **Step 4: Propagate non-disabled NetworkPolicy errors after status patch**

Immediately after the existing `if let Some(Err(e)) = metrics_outcome { … }` block (the one that propagates non-condition-mapped metrics errors), add a similar block for `np_outcome`:

```rust
    if let Some(Err(e)) = np_outcome {
        return Err(e);
    }
```

- [ ] **Step 5: Update every literal `KafkaSpec { … }` in `tests/reconcile_kafka.rs`**

Run: `grep -n "KafkaSpec {" crates/operator/tests/reconcile_kafka.rs`

Each literal needs `network_policy: None,` appended after `metrics_config: …,`. For the new tests below that need `Some(NetworkPolicySpec::default())`, use the new helper introduced in Step 6.

- [ ] **Step 6: Add a `kafka_cr_with_network_policy` helper**

In `tests/reconcile_kafka.rs`, alongside the existing `kafka_cr_with_metrics` helper, add:

```rust
/// Variant carrying `spec.networkPolicy` for slice-23 tests.
fn kafka_cr_with_network_policy(
    name: &str,
    namespace: &str,
    network_policy: Option<crabka_operator::crd::NetworkPolicySpec>,
) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}
```

Add the import: `NetworkPolicySpec` to the existing `use crabka_operator::crd::{...}` line so the call site compiles.

- [ ] **Step 7: Add the five reconcile tests**

Append to `tests/reconcile_kafka.rs`:

```rust
/// Slice 23: `spec.networkPolicy=None` (the default in `kafka_cr`)
/// must not touch `/networkpolicies/` at all and must surface
/// `NetworkPolicyReady=False reason=Disabled`.
#[tokio::test]
async fn network_policy_disabled_no_apply() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr("demo", "y");
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    for r in &observed {
        let uri = r.uri().to_string();
        assert!(
            !uri.contains("/networkpolicies/"),
            "networkPolicy=None must not touch /networkpolicies/: {uri}",
        );
    }

    // NetworkPolicyReady=False reason=Disabled present.
    let status = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    let cond = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "NetworkPolicyReady")
        .expect("NetworkPolicyReady condition present");
    assert_eq!(cond["status"], "False", "body = {body}");
    assert_eq!(cond["reason"], "Disabled", "body = {body}");
}

/// Slice 23: `spec.networkPolicy=Some(NetworkPolicySpec::default())`
/// applies exactly one NetworkPolicy via SSA and surfaces
/// `NetworkPolicyReady=True reason=Available`.
#[tokio::test]
async fn network_policy_enabled_applies_one_resource() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    // Insert the NetworkPolicy apply rule before the trailing status PATCH.
    let mut rules = happy_path_rules("demo", "y", &items);
    let last_idx = rules.len() - 1;
    rules.insert(
        last_idx,
        MockRule {
            method: Method::PATCH,
            path_substr: "/networkpolicies/demo-broker-policy".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "metadata": {"name": "demo-broker-policy", "namespace": "y"},
                }),
            ),
        },
    );
    let (ctx, state) = build_ctx("y", rules);

    let kafka = kafka_cr_with_network_policy(
        "demo",
        "y",
        Some(crabka_operator::crd::NetworkPolicySpec::default()),
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let apply_count = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/networkpolicies/demo-broker-policy")
        })
        .count();
    assert_eq!(apply_count, 1, "exactly one NetworkPolicy PATCH");

    let status = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    let cond = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "NetworkPolicyReady")
        .expect("NetworkPolicyReady present");
    assert_eq!(cond["status"], "True", "body = {body}");
    assert_eq!(cond["reason"], "Available", "body = {body}");
    assert_eq!(state.remaining_rules(), 0);
}

/// Slice 23: a Kafka CR with `status.conditions[NetworkPolicyReady].reason
/// = "Available"` and `spec.networkPolicy = None` issues exactly one
/// DELETE on `<name>-broker-policy` (orphan cleanup).
#[tokio::test]
async fn network_policy_transition_deletes_on_disable() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    let last_idx = rules.len() - 1;
    rules.insert(
        last_idx,
        MockRule {
            method: Method::DELETE,
            path_substr: "/networkpolicies/demo-broker-policy".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "kind": "Status", "apiVersion": "v1", "status": "Success",
                }),
            ),
        },
    );
    let (ctx, state) = build_ctx("y", rules);

    // Build a Kafka whose cached status already carries
    // NetworkPolicyReady=Available.
    let mut kafka = kafka_cr("demo", "y");
    kafka.status = Some(crabka_operator::crd::KafkaStatus {
        conditions: vec![crabka_operator::crd::KafkaCondition {
            type_: "NetworkPolicyReady".into(),
            status: "True".into(),
            reason: "Available".into(),
            message: "previously rendered".into(),
            last_transition_time: "2026-05-17T00:00:00Z".into(),
        }],
        replicas: Some(1),
        ready_replicas: Some(1),
        listeners: vec![],
    });

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let deletes: Vec<_> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::DELETE
                && r.uri().to_string().contains("/networkpolicies/demo-broker-policy")
        })
        .collect();
    assert_eq!(deletes.len(), 1, "exactly one DELETE call on transition");
}

/// Slice 23: cold disable (no prior `NetworkPolicyReady=Available`) must
/// not call DELETE at all — avoids gratuitous API calls for clusters that
/// never opted into NetworkPolicy.
#[tokio::test]
async fn network_policy_cold_disable_no_delete() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr("demo", "y"); // no status, no networkPolicy
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let deletes_or_patches: Vec<_> = observed
        .iter()
        .filter(|r| r.uri().to_string().contains("/networkpolicies/"))
        .collect();
    assert!(
        deletes_or_patches.is_empty(),
        "cold disable must not touch /networkpolicies/",
    );
}

/// Slice 23: when one listener has `network_policy_peers=Some(vec![])`,
/// the rendered NetworkPolicy body sent on the PATCH must NOT contain a
/// per-listener rule with empty `from` for that listener's port. (The
/// operator-allow rule for that port is still present.)
#[tokio::test]
async fn network_policy_listener_deny_all_skips_port_rule() {
    use crabka_operator::crd::{Listener, ListenerType, NetworkPolicySpec};

    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    let last_idx = rules.len() - 1;
    rules.insert(
        last_idx,
        MockRule {
            method: Method::PATCH,
            path_substr: "/networkpolicies/demo-broker-policy".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "metadata": {"name": "demo-broker-policy", "namespace": "y"},
                }),
            ),
        },
    );
    let (ctx, state) = build_ctx("y", rules);

    let mut kafka = kafka_cr_with_network_policy("demo", "y", Some(NetworkPolicySpec::default()));
    kafka.spec.listeners = vec![Listener {
        name: "PLAIN".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: false,
        configuration: None,
        network_policy_peers: Some(vec![]),
    }];
    kafka.spec.inter_broker_listener_name = Some("PLAIN".into());

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let np_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/networkpolicies/demo-broker-policy")
        })
        .expect("NetworkPolicy PATCH captured");
    let body: serde_json::Value = serde_json::from_slice(np_patch.body()).unwrap();
    let ingress = body["spec"]["ingress"].as_array().expect("ingress array");

    // Count rules targeting :9092 with an empty `from` (would indicate
    // allow-all sneaking through for the deny-all listener).
    let allow_alls: Vec<_> = ingress
        .iter()
        .filter(|r| {
            let ports_match = r["ports"]
                .as_array()
                .map(|ps| {
                    ps.iter().any(|p| p["port"].as_i64() == Some(9092))
                })
                .unwrap_or(false);
            let from_empty = r["from"].as_array().map(|fs| fs.is_empty()).unwrap_or(false);
            ports_match && from_empty
        })
        .collect();
    assert!(
        allow_alls.is_empty(),
        "deny-all listener (peers=[]) must not emit an allow-all rule, body = {body}",
    );
}
```

- [ ] **Step 8: Run all operator tests**

Run: `cargo test -p crabka-operator`

Expected: existing tests still pass plus 5 new reconcile tests pass.

- [ ] **Step 9: Run clippy**

Run: `cargo clippy -p crabka-operator --all-targets -- -D warnings`

Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add crates/operator/src/controller/kafka.rs \
        crates/operator/tests/reconcile_kafka.rs
git commit -m "Slice 23 T3: wire reconcile_network_policy + 5 reconcile tests"
```

---

## Task 4 — Helm RBAC for `networkpolicies`

**Files:**
- Modify: `charts/crabka-operator/templates/clusterrole.yaml`

- [ ] **Step 1: Add the rule block**

After the existing `monitoring.coreos.com` rule (the last block in `rules:`), append:

```yaml
  - apiGroups: ["networking.k8s.io"]
    resources: ["networkpolicies"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

- [ ] **Step 2: Lint the chart**

Run: `helm lint charts/crabka-operator`

Expected: `helm lint` reports 0 errors.

- [ ] **Step 3: Render with default values**

Run: `helm template charts/crabka-operator | grep -A2 networkpolicies`

Expected: the rendered ClusterRole contains the new rule block exactly as written.

- [ ] **Step 4: Commit**

```bash
git add charts/crabka-operator/templates/clusterrole.yaml
git commit -m "Slice 23 T4: ClusterRole grants networking.k8s.io/networkpolicies"
```

---

## Task 5 — Regenerate CRD YAML

**Depends on:** Task 1.

**Files:**
- Modify (regen): `deploy/crds/crabka.io_kafkas.yaml`

- [ ] **Step 1: Regenerate**

Run: `./tools/regen-crds.sh`

(The script invokes `cargo run -p crabka-operator -- gen-crds deploy/crds`. On Windows where bash isn't available, run that command directly.)

Expected: `deploy/crds/crabka.io_kafkas.yaml` gains `networkPolicy` under `spec` and `networkPolicyPeers` under each listener entry. `crabka.io_kafkanodepools.yaml` is unchanged.

- [ ] **Step 2: Verify the diff is schema-only**

Run: `git diff deploy/crds/crabka.io_kafkas.yaml | head -80`

Expected: additions only — new property descriptions under `spec.properties.networkPolicy` and `spec.properties.listeners.items.properties.networkPolicyPeers`.

- [ ] **Step 3: Re-run gen-crds to confirm stability**

Run: `./tools/regen-crds.sh && git diff --quiet deploy/crds && echo CLEAN || echo DIRTY`

Expected: `CLEAN` (no diff after the second run).

- [ ] **Step 4: Commit**

```bash
git add deploy/crds/crabka.io_kafkas.yaml
git commit -m "Slice 23 T5: regenerate Kafka CRD with networkPolicy fields"
```

---

## Task 6 — Operator e2e: Calico CNI + peer-restricted listener test

**Depends on:** Task 3 (e2e asserts the `NetworkPolicyReady` condition).

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

**Strategy:** kindnet does not enforce `NetworkPolicy`. Add a NEW job `kind-network-policy` (in the same workflow file, separate from the existing `kind` and `kind-upgrade` jobs) that creates a fresh kind cluster with Calico, installs the chart, applies a Kafka with peer-restricted listener, and asserts allow-vs-deny behaviour. Leave the existing `kind` job using kindnet so its tests aren't disturbed by the CNI change.

- [ ] **Step 1: Append a new job `kind-network-policy` to `.github/workflows/operator-e2e.yml`**

Append the following block to the end of the file (after the `kind-upgrade` job, at the same indentation as `kind:` and `kind-upgrade:`):

```yaml
  kind-network-policy:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.95.0"
      - uses: azure/setup-helm@v5
        with:
          version: v3.16.2
      - uses: actions/setup-go@v6
        with:
          go-version: stable

      - name: Install melange and apko
        run: |
          go install chainguard.dev/melange@latest
          go install chainguard.dev/apko@latest
          echo "$HOME/go/bin" >> "$GITHUB_PATH"

      - name: Build crabka-operator + crabka-broker images
        run: |
          mkdir -p packages
          melange keygen
          melange build packaging/melange/crabka-operator.yaml \
            --source-dir . --signing-key melange.rsa \
            --arch x86_64 --runner docker --out-dir packages/
          apko build packaging/apko/crabka-operator.yaml \
            crabka-operator:e2e crabka-operator.tar \
            --arch x86_64 \
            --repository-append "$PWD/packages" \
            --keyring-append "$PWD/melange.rsa.pub"
          melange build packaging/melange/crabka-broker.yaml \
            --source-dir . --signing-key melange.rsa \
            --arch x86_64 --runner docker --out-dir packages/
          apko build packaging/apko/crabka-broker.yaml \
            crabka-broker:e2e crabka-broker.tar \
            --arch x86_64 \
            --repository-append "$PWD/packages" \
            --keyring-append "$PWD/melange.rsa.pub"

      - name: Create kind cluster (Calico, no default CNI)
        run: |
          cat <<EOF > /tmp/kind-config.yaml
          kind: Cluster
          apiVersion: kind.x-k8s.io/v1alpha4
          name: crabka-np-e2e
          networking:
            disableDefaultCNI: true
            podSubnet: "192.168.0.0/16"
          nodes:
          - role: control-plane
          EOF
          kind create cluster --config /tmp/kind-config.yaml \
            --image kindest/node:v1.30.0 \
            --wait 60s || true

      - name: Install Calico
        run: |
          set -e
          # Pinned to a known-good release; renovate can keep this fresh.
          # Renovate: datasource=github-tags depName=projectcalico/calico
          CALICO_TAG=v3.28.2
          kubectl apply --server-side --force-conflicts \
            -f "https://raw.githubusercontent.com/projectcalico/calico/${CALICO_TAG}/manifests/tigera-operator.yaml"
          # Wait for the tigera-operator deployment to come up so the
          # `Installation` CRD is admitted.
          kubectl rollout status -n tigera-operator deploy/tigera-operator --timeout=180s

          cat <<EOF | kubectl apply -f -
          apiVersion: operator.tigera.io/v1
          kind: Installation
          metadata:
            name: default
          spec:
            calicoNetwork:
              ipPools:
              - blockSize: 26
                cidr: 192.168.0.0/16
                encapsulation: VXLANCrossSubnet
          EOF

          # Wait for calico-node DaemonSet to be ready on every node.
          for i in $(seq 1 60); do
            ready=$(kubectl get ds -n calico-system calico-node \
              -o jsonpath='{.status.numberReady}' 2>/dev/null || echo 0)
            desired=$(kubectl get ds -n calico-system calico-node \
              -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || echo 1)
            echo "calico-node ready=$ready desired=$desired"
            if [ "$ready" != "0" ] && [ "$ready" = "$desired" ]; then exit 0; fi
            sleep 5
          done
          echo "::error::calico-node never became Ready"
          kubectl get pods -n calico-system
          exit 1

      - name: Load operator + broker images into kind
        run: |
          set -e
          for tar in crabka-operator.tar crabka-broker.tar; do
            docker load -i "$tar" 2>&1 | tee /tmp/load.log
            loaded=$(sed -n 's/^Loaded image: //p' /tmp/load.log | head -1)
            want=$(basename "$tar" .tar):e2e
            if [ "$loaded" != "$want" ]; then docker tag "$loaded" "$want"; fi
            kind load docker-image "$want" --name crabka-np-e2e
          done

      - name: Install CRDs + chart
        run: |
          set -e
          kubectl apply -f deploy/crds/crabka.io_kafkas.yaml
          kubectl apply -f deploy/crds/crabka.io_kafkanodepools.yaml
          kubectl create namespace crabka-operator
          helm install operator charts/crabka-operator \
            --namespace crabka-operator \
            --set image.repository=crabka-operator --set image.tag=e2e \
            --set image.pullPolicy=IfNotPresent \
            --set brokerImage.repository=crabka-broker --set brokerImage.tag=e2e \
            --set brokerImage.pullPolicy=IfNotPresent
          kubectl rollout status -n crabka-operator deploy/operator-crabka-operator --timeout=240s

      - name: Create labeled and unlabeled client namespaces
        run: |
          set -e
          kubectl create namespace clients
          kubectl label namespace clients role=clients
          # `default` is already present and unlabeled.

      - name: Apply Kafka with networkPolicy + peer-restricted listener
        run: |
          set -e
          cat <<'EOF' | kubectl apply -f -
          apiVersion: crabka.io/v1alpha1
          kind: Kafka
          metadata: { name: demo, namespace: default }
          spec:
            kafkaVersion: "0.1.1"
            listeners:
            - name: PLAIN
              port: 9092
              type: internal
              tls: false
              networkPolicyPeers:
              - namespaceSelector:
                  matchLabels:
                    role: clients
            interBrokerListenerName: PLAIN
            networkPolicy: {}
          ---
          apiVersion: crabka.io/v1alpha1
          kind: KafkaNodePool
          metadata:
            name: brokers
            namespace: default
            labels: { crabka.io/cluster: demo }
          spec:
            roles: [Controller, Broker]
            replicas: 1
            nodeIdStart: 0
          EOF

      - name: Wait NetworkPolicyReady=True
        run: |
          for i in $(seq 1 60); do
            s=$(kubectl get kafka demo -n default \
              -o jsonpath='{.status.conditions[?(@.type=="NetworkPolicyReady")].status}' 2>/dev/null || true)
            echo "attempt $i: NetworkPolicyReady=$s"
            if [ "$s" = "True" ]; then exit 0; fi
            sleep 5
          done
          echo "::error::NetworkPolicyReady never True"
          kubectl describe kafka demo -n default
          kubectl logs -n crabka-operator deploy/operator-crabka-operator --tail=200 || true
          exit 1

      - name: Verify the NetworkPolicy is present
        run: |
          set -e
          kubectl get networkpolicy demo-broker-policy -n default
          kubectl get networkpolicy demo-broker-policy -n default -o yaml

      - name: Wait for broker Ready
        run: |
          for i in $(seq 1 60); do
            phase=$(kubectl get pod demo-brokers-0 -n default \
              -o jsonpath='{.status.phase}' 2>/dev/null || true)
            cond=$(kubectl get pod demo-brokers-0 -n default \
              -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
            echo "attempt $i: phase=$phase ready=$cond"
            if [ "$cond" = "True" ]; then exit 0; fi
            sleep 5
          done
          echo "::error::broker pod never became Ready"
          kubectl describe pod demo-brokers-0 -n default
          exit 1

      - name: Probe from a LABELED namespace (must succeed)
        run: |
          set -e
          kubectl run nc-allowed -n clients --image=alpine:3 --restart=Never -- \
            sh -c 'apk add --no-cache busybox-extras > /dev/null && \
                   nc -zv -w 5 demo-broker-headless.default.svc.cluster.local 9092'
          # Wait for completion; expect exit 0.
          for i in $(seq 1 30); do
            status=$(kubectl get pod nc-allowed -n clients -o jsonpath='{.status.phase}')
            echo "nc-allowed status=$status"
            if [ "$status" = "Succeeded" ]; then exit 0; fi
            if [ "$status" = "Failed" ]; then
              kubectl logs -n clients nc-allowed
              echo "::error::labeled-ns client should have reached broker"
              exit 1
            fi
            sleep 3
          done
          echo "::error::nc-allowed never completed"
          kubectl logs -n clients nc-allowed
          exit 1

      - name: Probe from an UNLABELED namespace (must fail)
        run: |
          set -e
          # Run with a short connect timeout so the probe terminates cleanly.
          kubectl run nc-denied -n default --image=alpine:3 --restart=Never -- \
            sh -c 'apk add --no-cache busybox-extras > /dev/null && \
                   nc -zv -w 5 demo-broker-headless.default.svc.cluster.local 9092; echo exit=$?'
          for i in $(seq 1 30); do
            status=$(kubectl get pod nc-denied -n default -o jsonpath='{.status.phase}')
            echo "nc-denied status=$status"
            if [ "$status" = "Succeeded" ] || [ "$status" = "Failed" ]; then break; fi
            sleep 3
          done
          logs=$(kubectl logs -n default nc-denied)
          echo "$logs"
          # Calico drops the SYN: nc reports "Connection timed out" and the
          # `exit=` line is non-zero (commonly 1).
          if echo "$logs" | grep -q 'exit=0'; then
            echo "::error::unlabeled-ns client unexpectedly connected"
            exit 1
          fi

      - name: Collect diagnostics on failure
        if: failure()
        run: |
          set +e
          mkdir -p /tmp/np-diag
          {
            echo "## kind-network-policy diagnostics"
            kubectl get pods -A -o wide
            echo "### operator logs"
            kubectl logs -n crabka-operator deploy/operator-crabka-operator --tail=500
            echo "### kafka CR"
            kubectl get kafka demo -n default -o yaml
            echo "### NetworkPolicy"
            kubectl get networkpolicy demo-broker-policy -n default -o yaml
            echo "### broker logs"
            kubectl logs -n default demo-brokers-0 --tail=200 --all-containers
            echo "### nc-allowed logs"
            kubectl logs -n clients nc-allowed
            echo "### nc-denied logs"
            kubectl logs -n default nc-denied
          } > /tmp/np-diag/diagnostics.md

      - name: Upload diagnostics
        if: failure()
        uses: actions/upload-artifact@v7
        with:
          name: operator-e2e-network-policy-diagnostics
          path: /tmp/np-diag/
          retention-days: 14
          if-no-files-found: ignore
```

- [ ] **Step 2: Validate the workflow file is syntactically valid YAML**

Run: `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/operator-e2e.yml'))"`

Expected: no output (parsed cleanly).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/operator-e2e.yml
git commit -m "Slice 23 T6: operator-e2e kind-network-policy job with Calico"
```

---

## Final verification

After all six tasks land:

- [ ] **Build:**

Run: `cargo build -p crabka-operator`
Expected: clean.

- [ ] **Tests:**

Run: `cargo test -p crabka-operator`
Expected: all green, including ~16 new tests (3 CRD + 11 controller + 5 reconcile).

- [ ] **Clippy:**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **CRD regen stability:**

Run: `./tools/regen-crds.sh && git diff --quiet deploy/crds && echo CLEAN`
Expected: `CLEAN`.

- [ ] **Helm:**

Run: `helm lint charts/crabka-operator`
Expected: 0 errors.

- [ ] **Workflow parse:**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/operator-e2e.yml'))"`
Expected: no output.

---

## Acceptance criteria (mirrors spec §9)

1. `cargo build -p crabka-operator` clean.
2. `cargo test -p crabka-operator` green, with ~16 new tests added across the layers.
3. `cargo clippy --workspace --all-targets -- -D warnings` clean.
4. `./tools/regen-crds.sh` produces no diff after the first run.
5. `helm lint charts/crabka-operator` passes.
6. operator-e2e `kind-network-policy` job (Calico): peer-restricted listener blocks unlabeled clients and allows labeled clients; `kubectl get networkpolicy demo-broker-policy -o yaml` shows the rendered rules; `NetworkPolicyReady=True reason=Available` set.
7. Upgrade smoke: pre-existing Kafka without `networkPolicy` does not roll any broker pods on chart upgrade; `NetworkPolicyReady=False reason=Disabled` is set (covered by the existing `kind-upgrade` job; no extra assertion required since the slice-21 hash function is untouched).
