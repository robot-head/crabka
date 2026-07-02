//! `NetworkPolicy` reconcile — opt-in via `Kafka.spec.networkPolicy`.
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
    NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer as K8sPeer, NetworkPolicyPort,
    NetworkPolicySpec as K8sNpSpec,
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

/// Render the `NetworkPolicy`. Pure function of the Kafka CR + effective
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
            pod_selector: Some(pod_selector),
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
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(cluster = %name, namespace = %namespace, inter_broker_port),
)]
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
            match np_api
                .delete(&format!("{name}-broker-policy"), &DeleteParams::default())
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(status)) if status.code == 404 => {}
                Err(e) => {
                    tracing::warn!(error = %e, "failed to delete orphaned NetworkPolicy");
                }
            }
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
    use assert2::assert;

    fn test_kafka() -> Kafka {
        let mut k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: Some(NetworkPolicySpec::default()),
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
                tiered_storage: None,
                inter_broker_kerberos: None,
                krb5_conf_secret_ref: None,
                tracing: None,
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
            authentication: None,
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
                            ps.iter().any(|p| p.port == Some(IntOrString::Int(port)))
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
        assert!(from.len() == 1);
        let pod = from[0].pod_selector.as_ref().unwrap();
        let labels = pod.match_labels.as_ref().unwrap();
        assert!(labels.get("app.kubernetes.io/name").map(String::as_str) == Some(APP_LABEL));
        assert!(labels.get("app.kubernetes.io/instance").map(String::as_str) == Some("demo"));
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
        assert!(operator_rules.len() == 2);
        let ports: Vec<i32> = operator_rules
            .iter()
            .map(|r| match &r.ports.as_ref().unwrap()[0].port {
                Some(IntOrString::Int(p)) => *p,
                _ => panic!("expected int port"),
            })
            .collect();
        assert!(
            ports.contains(&9092) && ports.contains(&9094),
            "ports={ports:?}"
        );
    }

    #[test]
    fn render_unset_peers_listener_emits_allow_all() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        // Filter for the per-listener rule (not the operator-allow, not the inter-broker self_peer).
        let rules_on_9092 = rules_targeting_port(&np, 9092);
        // Expected rules on 9092: inter-broker self_peer, operator-allow, per-listener allow-all.
        // The per-listener allow-all has an empty `from`.
        let allow_all = rules_on_9092
            .iter()
            .find(|r| r.from.as_ref().is_some_and(std::vec::Vec::is_empty));
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
        let allow_all = rules_on_9092
            .iter()
            .find(|r| r.from.as_ref().is_some_and(std::vec::Vec::is_empty));
        assert!(
            allow_all.is_none(),
            "deny-all listener must not emit an allow-all (empty `from`) rule"
        );
        // Exactly two rules on 9092: inter-broker + operator-allow.
        assert!(rules_on_9092.len() == 2);
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
        assert!(
            restricted.is_some(),
            "expected per-listener restricted rule"
        );
    }

    #[test]
    fn render_metrics_enabled_emits_metrics_port_rule() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, true).unwrap();
        let rules_on_9404 = rules_targeting_port(&np, METRICS_PORT);
        assert!(rules_on_9404.len() == 1);
        // Allow-all on metrics (empty `from`).
        assert!(
            rules_on_9404[0]
                .from
                .as_ref()
                .is_some_and(std::vec::Vec::is_empty)
        );
    }

    #[test]
    fn render_metrics_disabled_no_metrics_port_rule() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let rules_on_9404 = rules_targeting_port(&np, METRICS_PORT);
        assert!(
            rules_on_9404.is_empty(),
            "no metrics rule when metricsConfig unset"
        );
    }

    #[test]
    fn render_pod_selector_matches_pool_pods() {
        let listeners = vec![internal_listener("PLAIN", BROKER_PORT, None)];
        let np = render_network_policy(&test_kafka(), &listeners, BROKER_PORT, false).unwrap();
        let sel = np
            .spec
            .as_ref()
            .unwrap()
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert!(sel.get("app.kubernetes.io/name").map(String::as_str) == Some(APP_LABEL));
        assert!(sel.get("app.kubernetes.io/instance").map(String::as_str) == Some("demo"));
    }

    #[test]
    fn render_policy_types_ingress_only() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let spec = np.spec.as_ref().unwrap();
        assert!(spec.policy_types.as_ref().unwrap() == &vec!["Ingress".to_string()]);
        assert!(spec.egress.is_none());
    }

    #[test]
    fn render_name_and_namespace() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        assert!(np.metadata.name.as_deref() == Some("demo-broker-policy"));
        assert!(np.metadata.namespace.as_deref() == Some("default"));
    }

    #[test]
    fn render_owner_ref_set() {
        let listeners = vec![internal_listener("PLAIN", 9092, None)];
        let np = render_network_policy(&test_kafka(), &listeners, 9092, false).unwrap();
        let refs = np.metadata.owner_references.as_ref().unwrap();
        assert!(
            refs == &vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    api_version: "crabka.io/v1alpha1".into(),
                    block_owner_deletion: Some(true),
                    controller: Some(true),
                    kind: "Kafka".into(),
                    name: "demo".into(),
                    uid: "00000000-0000-0000-0000-000000000001".into(),
                }
            ]
        );
    }
}
