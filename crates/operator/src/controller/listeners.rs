//! Listener-related rendering and validation. Kept in its own module
//! to keep `controller/kafka.rs` and `controller/common.rs` from
//! growing further.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Service;
use kube::Resource as _;

use crate::controller::common::{APP_LABEL, ReconcileError, owner_ref};
use crate::crd::{Kafka, Listener, ListenerType};

/// Reason values for the `ListenersValid` status condition.
/// Stable strings — consumed by `kubectl wait --for=condition=…` and
/// asserted by tests.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    DuplicateListenerName(String),
    DuplicateListenerPort(i32),
    TlsNotYetSupported(String),
    IngressDeferred(String),
    RouteDeferred(String),
    DuplicateBrokerOverride { listener: String, broker: i32 },
    InterBrokerListenerMissing(String),
    InterBrokerListenerNotInternal(String),
    NoInternalListener,
}

#[allow(dead_code)]
impl ValidationError {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::DuplicateListenerName(_) => "DuplicateListenerName",
            Self::DuplicateListenerPort(_) => "DuplicateListenerPort",
            Self::TlsNotYetSupported(_) => "TlsNotYetSupported",
            Self::IngressDeferred(_) => "IngressDeferred",
            Self::RouteDeferred(_) => "RouteDeferred",
            Self::DuplicateBrokerOverride { .. } => "DuplicateBrokerOverride",
            Self::InterBrokerListenerMissing(_) => "InterBrokerListenerMissing",
            Self::InterBrokerListenerNotInternal(_) => "InterBrokerListenerNotInternal",
            Self::NoInternalListener => "NoInternalListener",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::DuplicateListenerName(n) => {
                format!("listener name '{n}' is used more than once")
            }
            Self::DuplicateListenerPort(p) => {
                format!("listener port {p} is used more than once")
            }
            Self::TlsNotYetSupported(n) => {
                format!("listener '{n}' has tls=true; TLS arrives in Phase 4")
            }
            Self::IngressDeferred(n) => {
                format!("listener '{n}' has type=ingress; reconcile is deferred until slice 27")
            }
            Self::RouteDeferred(n) => {
                format!("listener '{n}' has type=route; reconcile is deferred until slice 27")
            }
            Self::DuplicateBrokerOverride { listener, broker } => format!(
                "listener '{listener}' has duplicate configuration.brokers entries for broker {broker}"
            ),
            Self::InterBrokerListenerMissing(n) => {
                format!("spec.interBrokerListenerName='{n}' does not match any listener")
            }
            Self::InterBrokerListenerNotInternal(n) => {
                format!("spec.interBrokerListenerName='{n}' points to a non-internal listener")
            }
            Self::NoInternalListener => {
                "spec.listeners is non-empty but contains no internal-type listener".into()
            }
        }
    }
}

/// Validate `spec.listeners` + `spec.interBrokerListenerName`. Returns
/// `Ok(())` if everything is well-formed; otherwise the first error
/// encountered (validation is short-circuit — surface the most
/// actionable problem rather than a list).
#[allow(dead_code)]
pub fn validate_listeners(
    listeners: &[Listener],
    inter_broker_listener_name: Option<&str>,
) -> Result<(), ValidationError> {
    // Duplicate name / port checks.
    for (i, l) in listeners.iter().enumerate() {
        for prior in &listeners[..i] {
            if prior.name == l.name {
                return Err(ValidationError::DuplicateListenerName(l.name.clone()));
            }
            if prior.port == l.port {
                return Err(ValidationError::DuplicateListenerPort(l.port));
            }
        }
    }

    // Per-listener type/tls/override checks.
    for l in listeners {
        if l.tls {
            return Err(ValidationError::TlsNotYetSupported(l.name.clone()));
        }
        match l.type_ {
            ListenerType::Ingress => {
                return Err(ValidationError::IngressDeferred(l.name.clone()));
            }
            ListenerType::Route => {
                return Err(ValidationError::RouteDeferred(l.name.clone()));
            }
            _ => {}
        }
        if let Some(cfg) = &l.configuration {
            let mut seen = std::collections::HashSet::new();
            for ovr in &cfg.brokers {
                if !seen.insert(ovr.broker) {
                    return Err(ValidationError::DuplicateBrokerOverride {
                        listener: l.name.clone(),
                        broker: ovr.broker,
                    });
                }
            }
        }
    }

    // Inter-broker listener resolution.
    if !listeners.is_empty() {
        let has_internal = listeners.iter().any(|l| l.type_ == ListenerType::Internal);
        if !has_internal {
            return Err(ValidationError::NoInternalListener);
        }
        if let Some(name) = inter_broker_listener_name {
            match listeners.iter().find(|l| l.name == name) {
                None => return Err(ValidationError::InterBrokerListenerMissing(name.into())),
                Some(l) if l.type_ != ListenerType::Internal => {
                    return Err(ValidationError::InterBrokerListenerNotInternal(name.into()));
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Pick the inter-broker listener name. Honors an explicit override;
/// otherwise picks the first `internal` listener. Returns the synthesized
/// default name (`"PLAIN"`) when `listeners` is empty (the slice-19
/// compatibility path).
#[allow(dead_code)]
#[must_use]
pub fn effective_inter_broker_listener_name(
    listeners: &[Listener],
    explicit: Option<&str>,
) -> String {
    if let Some(s) = explicit {
        return s.to_string();
    }
    if listeners.is_empty() {
        return "PLAIN".to_string();
    }
    listeners
        .iter()
        .find(|l| l.type_ == ListenerType::Internal)
        .map_or_else(|| "PLAIN".to_string(), |l| l.name.clone())
}

/// Render the per-broker external Service for the given listener +
/// broker id. The Service's selector uses the built-in
/// `statefulset.kubernetes.io/pod-name` label (K8s 1.28+) to pin it
/// to exactly the pod that hosts this broker.
///
/// `pod_name` is the StatefulSet-allocated pod name (e.g.
/// `demo-controller-0`). Caller computes it from pool+ordinal.
///
/// # Panics
///
/// Panics if called with `internal`, `ingress`, or `route` listener
/// types. Callers must filter to `nodeport`/`loadbalancer` first.
#[allow(dead_code)]
pub fn render_broker_service(
    owner: &Kafka,
    listener: &Listener,
    broker_id: i32,
    pod_name: &str,
) -> Result<Service, ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let namespace = owner.meta().namespace.clone();
    let svc_name = format!("{cluster_name}-{}-{broker_id}", listener.name);

    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    labels.insert("app.kubernetes.io/instance".into(), cluster_name.clone());
    labels.insert("crabka.io/listener".into(), listener.name.clone());
    labels.insert("crabka.io/broker".into(), broker_id.to_string());

    let mut selector: BTreeMap<String, String> = BTreeMap::new();
    selector.insert(
        "statefulset.kubernetes.io/pod-name".into(),
        pod_name.to_string(),
    );

    let service_type = match listener.type_ {
        ListenerType::Nodeport => "NodePort",
        ListenerType::Loadbalancer => "LoadBalancer",
        _ => panic!(
            "render_broker_service called with non-external type {:?}",
            listener.type_
        ),
    };

    let override_ = listener
        .configuration
        .as_ref()
        .and_then(|c| c.brokers.iter().find(|b| b.broker == broker_id));

    let mut port = serde_json::json!({
        "name": listener.name,
        "port": listener.port,
        "targetPort": listener.port,
        "protocol": "TCP",
    });
    if let Some(np) = override_.and_then(|o| o.node_port) {
        port["nodePort"] = serde_json::json!(np);
    }
    let mut spec = serde_json::json!({
        "type": service_type,
        "selector": selector,
        "ports": [port],
    });
    if let Some(lb_ip) = override_.and_then(|o| o.load_balancer_ip.as_ref()) {
        spec["loadBalancerIP"] = serde_json::json!(lb_ip);
    }

    let svc: Service = serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": svc_name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": spec,
    }))?;
    Ok(svc)
}

/// Render the bootstrap Service for the given external listener. Its
/// selector matches every broker pod of the cluster.
///
/// # Panics
///
/// Panics if called with `internal`, `ingress`, or `route` listener types.
#[allow(dead_code)]
pub fn render_bootstrap_service(
    owner: &Kafka,
    listener: &Listener,
) -> Result<Service, ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let namespace = owner.meta().namespace.clone();
    let svc_name = format!("{cluster_name}-{}-bootstrap", listener.name);

    let bootstrap = listener
        .configuration
        .as_ref()
        .and_then(|c| c.bootstrap.as_ref());

    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    labels.insert("app.kubernetes.io/instance".into(), cluster_name.clone());
    labels.insert("crabka.io/listener".into(), listener.name.clone());
    labels.insert("crabka.io/role".into(), "bootstrap".into());
    if let Some(b) = bootstrap {
        for (k, v) in &b.labels {
            labels.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    let mut annotations: BTreeMap<String, String> = BTreeMap::new();
    if let Some(b) = bootstrap {
        for (k, v) in &b.annotations {
            annotations.insert(k.clone(), v.clone());
        }
    }

    let mut selector: BTreeMap<String, String> = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    selector.insert("app.kubernetes.io/instance".into(), cluster_name.clone());

    let service_type = match listener.type_ {
        ListenerType::Nodeport => "NodePort",
        ListenerType::Loadbalancer => "LoadBalancer",
        _ => panic!(
            "render_bootstrap_service called with non-external type {:?}",
            listener.type_
        ),
    };

    let mut port = serde_json::json!({
        "name": listener.name,
        "port": listener.port,
        "targetPort": listener.port,
        "protocol": "TCP",
    });
    if let Some(np) = bootstrap.and_then(|b| b.node_port) {
        port["nodePort"] = serde_json::json!(np);
    }
    let mut spec = serde_json::json!({
        "type": service_type,
        "selector": selector,
        "ports": [port],
    });
    if let Some(lb_ip) = bootstrap.and_then(|b| b.load_balancer_ip.as_ref()) {
        spec["loadBalancerIP"] = serde_json::json!(lb_ip);
    }

    let mut meta = serde_json::json!({
        "name": svc_name,
        "namespace": namespace,
        "labels": labels,
        "ownerReferences": [owner_ref::<Kafka>(owner)?],
    });
    if !annotations.is_empty() {
        meta["annotations"] = serde_json::to_value(&annotations)?;
    }

    let svc: Service = serde_json::from_value(serde_json::json!({
        "metadata": meta,
        "spec": spec,
    }))?;
    Ok(svc)
}

#[cfg(test)]
mod service_rendering_tests {
    use super::*;
    use crate::crd::{BootstrapConfig, BrokerOverride, KafkaSpec, ListenerConfiguration};

    fn kafka(name: &str) -> Kafka {
        let mut k = Kafka::new(
            name,
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
            },
        );
        k.meta_mut().namespace = Some("default".into());
        k.meta_mut().uid = Some("00000000-0000-0000-0000-000000000001".into());
        k
    }

    #[test]
    fn nodeport_broker_service_has_pod_name_selector_and_nodeport() {
        let k = kafka("demo");
        let listener = Listener {
            name: "external".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: false,
            configuration: Some(ListenerConfiguration {
                bootstrap: None,
                brokers: vec![BrokerOverride {
                    broker: 0,
                    node_port: Some(32100),
                    ..Default::default()
                }],
            }),
        };
        let svc = render_broker_service(&k, &listener, 0, "demo-pool-0").unwrap();
        assert_eq!(svc.metadata.name.as_deref(), Some("demo-external-0"));
        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.type_.as_deref(), Some("NodePort"));
        let sel = spec.selector.as_ref().unwrap();
        assert_eq!(
            sel.get("statefulset.kubernetes.io/pod-name"),
            Some(&"demo-pool-0".to_string())
        );
        assert_eq!(spec.ports.as_ref().unwrap()[0].port, 9094);
        assert_eq!(spec.ports.as_ref().unwrap()[0].node_port, Some(32100));
    }

    #[test]
    fn loadbalancer_broker_service_uses_lb_ip_override() {
        let k = kafka("demo");
        let listener = Listener {
            name: "lb".into(),
            port: 9094,
            type_: ListenerType::Loadbalancer,
            tls: false,
            configuration: Some(ListenerConfiguration {
                bootstrap: None,
                brokers: vec![BrokerOverride {
                    broker: 0,
                    load_balancer_ip: Some("10.0.0.5".into()),
                    ..Default::default()
                }],
            }),
        };
        let svc = render_broker_service(&k, &listener, 0, "demo-pool-0").unwrap();
        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.type_.as_deref(), Some("LoadBalancer"));
        assert_eq!(spec.load_balancer_ip.as_deref(), Some("10.0.0.5"));
    }

    #[test]
    fn bootstrap_service_selects_all_broker_pods() {
        let k = kafka("demo");
        let listener = Listener {
            name: "external".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: false,
            configuration: Some(ListenerConfiguration {
                bootstrap: Some(BootstrapConfig {
                    node_port: Some(32099),
                    ..Default::default()
                }),
                brokers: vec![],
            }),
        };
        let svc = render_bootstrap_service(&k, &listener).unwrap();
        assert_eq!(
            svc.metadata.name.as_deref(),
            Some("demo-external-bootstrap")
        );
        let spec = svc.spec.as_ref().unwrap();
        let sel = spec.selector.as_ref().unwrap();
        assert_eq!(
            sel.get("app.kubernetes.io/instance"),
            Some(&"demo".to_string())
        );
        assert!(sel.get("statefulset.kubernetes.io/pod-name").is_none());
        assert_eq!(spec.ports.as_ref().unwrap()[0].node_port, Some(32099));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{BrokerOverride, ListenerConfiguration};

    fn internal(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: false,
            configuration: None,
        }
    }

    fn nodeport(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Nodeport,
            tls: false,
            configuration: None,
        }
    }

    #[test]
    fn empty_listeners_is_valid() {
        assert!(validate_listeners(&[], None).is_ok());
    }

    #[test]
    fn one_internal_is_valid() {
        let ls = [internal("PLAIN", 9092)];
        assert!(validate_listeners(&ls, None).is_ok());
    }

    #[test]
    fn duplicate_name_is_rejected() {
        let ls = [internal("PLAIN", 9092), nodeport("PLAIN", 9094)];
        let err = validate_listeners(&ls, None).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateListenerName(_)));
        assert_eq!(err.reason(), "DuplicateListenerName");
    }

    #[test]
    fn duplicate_port_is_rejected() {
        let ls = [internal("A", 9092), nodeport("B", 9092)];
        let err = validate_listeners(&ls, None).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateListenerPort(9092)));
    }

    #[test]
    fn tls_true_is_rejected() {
        let mut l = internal("PLAIN", 9092);
        l.tls = true;
        assert_eq!(
            validate_listeners(&[l], None).unwrap_err().reason(),
            "TlsNotYetSupported"
        );
    }

    #[test]
    fn ingress_is_deferred() {
        let mut l = internal("ing", 9094);
        l.type_ = ListenerType::Ingress;
        assert_eq!(
            validate_listeners(&[l], None).unwrap_err().reason(),
            "IngressDeferred"
        );
    }

    #[test]
    fn route_is_deferred() {
        let mut l = internal("rt", 9094);
        l.type_ = ListenerType::Route;
        assert_eq!(
            validate_listeners(&[l], None).unwrap_err().reason(),
            "RouteDeferred"
        );
    }

    #[test]
    fn duplicate_broker_override_is_rejected() {
        let mut l = nodeport("ext", 9094);
        l.configuration = Some(ListenerConfiguration {
            bootstrap: None,
            brokers: vec![
                BrokerOverride {
                    broker: 0,
                    ..Default::default()
                },
                BrokerOverride {
                    broker: 0,
                    ..Default::default()
                },
            ],
        });
        let err = validate_listeners(&[l], None).unwrap_err();
        assert_eq!(err.reason(), "DuplicateBrokerOverride");
    }

    #[test]
    fn missing_internal_when_non_empty_is_rejected() {
        let ls = [nodeport("ext", 9094)];
        assert_eq!(
            validate_listeners(&ls, None).unwrap_err().reason(),
            "NoInternalListener"
        );
    }

    #[test]
    fn inter_broker_listener_must_match_a_listener() {
        let ls = [internal("PLAIN", 9092)];
        let err = validate_listeners(&ls, Some("MISSING")).unwrap_err();
        assert_eq!(err.reason(), "InterBrokerListenerMissing");
    }

    #[test]
    fn inter_broker_listener_must_be_internal() {
        let ls = [internal("PLAIN", 9092), nodeport("ext", 9094)];
        let err = validate_listeners(&ls, Some("ext")).unwrap_err();
        assert_eq!(err.reason(), "InterBrokerListenerNotInternal");
    }

    #[test]
    fn effective_name_explicit_wins() {
        assert_eq!(
            effective_inter_broker_listener_name(&[], Some("FOO")),
            "FOO"
        );
    }

    #[test]
    fn effective_name_picks_first_internal() {
        let ls = [
            nodeport("ext", 9094),
            internal("ib", 9092),
            internal("other", 9095),
        ];
        assert_eq!(effective_inter_broker_listener_name(&ls, None), "ib");
    }

    #[test]
    fn effective_name_empty_defaults_to_plain() {
        assert_eq!(effective_inter_broker_listener_name(&[], None), "PLAIN");
    }
}
