//! Listener-related rendering and validation. Kept in its own module
//! to keep `controller/kafka.rs` and `controller/common.rs` from
//! growing further.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Node, Service};
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
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
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
            authentication: None,
            configuration: Some(ListenerConfiguration {
                bootstrap: None,
                brokers: vec![BrokerOverride {
                    broker: 0,
                    node_port: Some(32100),
                    ..Default::default()
                }],
            }),
            network_policy_peers: None,
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
            authentication: None,
            configuration: Some(ListenerConfiguration {
                bootstrap: None,
                brokers: vec![BrokerOverride {
                    broker: 0,
                    load_balancer_ip: Some("10.0.0.5".into()),
                    ..Default::default()
                }],
            }),
            network_policy_peers: None,
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
            authentication: None,
            configuration: Some(ListenerConfiguration {
                bootstrap: Some(BootstrapConfig {
                    node_port: Some(32099),
                    ..Default::default()
                }),
                brokers: vec![],
            }),
            network_policy_peers: None,
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
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }

    fn nodeport(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Nodeport,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
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

/// Per-broker resolved advertised address.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct AdvertisedAddress {
    pub host: String,
    pub port: i32,
}

/// Errors that block advertised-listener computation. They map onto
/// `ListenersReady=False reason=PendingExternalAddresses`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum AdvertisedError {
    PodNotScheduled { broker: i32 },
    NodeNotFound { broker: i32, node_name: String },
    NodeHasNoAddress { broker: i32, node_name: String },
    ServiceMissing { broker: i32, service_name: String },
    NodePortNotAllocated { broker: i32 },
    LoadBalancerPending { broker: i32, service_name: String },
}

#[allow(dead_code)]
impl AdvertisedError {
    pub fn message(&self) -> String {
        match self {
            Self::PodNotScheduled { broker } => {
                format!("pod for broker {broker} not yet scheduled")
            }
            Self::NodeNotFound { broker, node_name } => {
                format!("node {node_name} for broker {broker} not visible")
            }
            Self::NodeHasNoAddress { broker, node_name } => {
                format!("node {node_name} for broker {broker} has no addresses")
            }
            Self::ServiceMissing {
                broker,
                service_name,
            } => {
                format!("service {service_name} for broker {broker} missing")
            }
            Self::NodePortNotAllocated { broker } => {
                format!("nodePort for broker {broker} not allocated yet")
            }
            Self::LoadBalancerPending {
                broker,
                service_name,
            } => {
                format!("loadBalancer for service {service_name} (broker {broker}) not provisioned")
            }
        }
    }
}

/// Compute the advertised host:port for one (listener, broker).
///
/// `pod_node_name` is `Pod.spec.nodeName` of the pod hosting this
/// broker (None if not yet scheduled). `nodes_by_name` is a map of
/// all Nodes the operator has observed. `per_broker_service` is the
/// per-broker Service the operator just rendered+applied (None until
/// the apiserver returns it).
///
/// # Panics
///
/// Panics if called with `Ingress` or `Route` listener types — validation
/// short-circuits those.
#[allow(dead_code)]
pub fn compute_advertised(
    listener: &Listener,
    broker_id: i32,
    pod_fqdn: &str,
    pod_node_name: Option<&str>,
    nodes_by_name: &std::collections::HashMap<String, Node>,
    per_broker_service: Option<&Service>,
) -> Result<AdvertisedAddress, AdvertisedError> {
    let override_ = listener
        .configuration
        .as_ref()
        .and_then(|c| c.brokers.iter().find(|b| b.broker == broker_id));

    match listener.type_ {
        ListenerType::Internal => Ok(AdvertisedAddress {
            host: pod_fqdn.to_string(),
            port: listener.port,
        }),
        ListenerType::Nodeport => {
            let host = if let Some(h) = override_.and_then(|o| o.advertised_host.as_ref()) {
                h.clone()
            } else {
                let node_name =
                    pod_node_name.ok_or(AdvertisedError::PodNotScheduled { broker: broker_id })?;
                let node =
                    nodes_by_name
                        .get(node_name)
                        .ok_or_else(|| AdvertisedError::NodeNotFound {
                            broker: broker_id,
                            node_name: node_name.to_string(),
                        })?;
                let addrs = node.status.as_ref().and_then(|s| s.addresses.as_ref());
                addrs
                    .and_then(|a| {
                        a.iter()
                            .find(|x| x.type_ == "ExternalIP")
                            .or_else(|| a.iter().find(|x| x.type_ == "InternalIP"))
                            .map(|x| x.address.clone())
                    })
                    .ok_or_else(|| AdvertisedError::NodeHasNoAddress {
                        broker: broker_id,
                        node_name: node_name.to_string(),
                    })?
            };
            let port = if let Some(p) = override_.and_then(|o| o.advertised_port) {
                p
            } else if let Some(p) = override_.and_then(|o| o.node_port) {
                p
            } else {
                let svc = per_broker_service.ok_or_else(|| AdvertisedError::ServiceMissing {
                    broker: broker_id,
                    service_name: String::new(),
                })?;
                svc.spec
                    .as_ref()
                    .and_then(|s| s.ports.as_ref())
                    .and_then(|ps| ps.first().and_then(|p| p.node_port))
                    .ok_or(AdvertisedError::NodePortNotAllocated { broker: broker_id })?
            };
            Ok(AdvertisedAddress { host, port })
        }
        ListenerType::Loadbalancer => {
            let svc_name = per_broker_service
                .and_then(|s| s.metadata.name.clone())
                .unwrap_or_default();
            let host = if let Some(h) = override_.and_then(|o| o.advertised_host.as_ref()) {
                h.clone()
            } else {
                let svc = per_broker_service.ok_or_else(|| AdvertisedError::ServiceMissing {
                    broker: broker_id,
                    service_name: String::new(),
                })?;
                let ingress = svc
                    .status
                    .as_ref()
                    .and_then(|st| st.load_balancer.as_ref())
                    .and_then(|lb| lb.ingress.as_ref())
                    .and_then(|ig| ig.first())
                    .ok_or_else(|| AdvertisedError::LoadBalancerPending {
                        broker: broker_id,
                        service_name: svc_name.clone(),
                    })?;
                ingress
                    .hostname
                    .clone()
                    .or_else(|| ingress.ip.clone())
                    .ok_or(AdvertisedError::LoadBalancerPending {
                        broker: broker_id,
                        service_name: svc_name,
                    })?
            };
            let port = override_
                .and_then(|o| o.advertised_port)
                .unwrap_or(listener.port);
            Ok(AdvertisedAddress { host, port })
        }
        ListenerType::Ingress | ListenerType::Route => {
            unreachable!(
                "compute_advertised called with deferred type {:?}",
                listener.type_
            )
        }
    }
}

#[cfg(test)]
mod advertised_tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        LoadBalancerIngress, LoadBalancerStatus, Node, NodeAddress, NodeStatus, Service,
        ServicePort, ServiceSpec, ServiceStatus,
    };
    use std::collections::HashMap;

    fn internal(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }
    fn nodeport(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Nodeport,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }
    fn loadbalancer(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Loadbalancer,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn internal_uses_pod_fqdn() {
        let l = internal("PLAIN", 9092);
        let nodes = HashMap::new();
        let a = compute_advertised(&l, 0, "pod.svc.local", None, &nodes, None).unwrap();
        assert_eq!(
            a,
            AdvertisedAddress {
                host: "pod.svc.local".into(),
                port: 9092
            }
        );
    }

    #[test]
    fn nodeport_pending_when_pod_unscheduled() {
        let l = nodeport("ext", 9094);
        let nodes = HashMap::new();
        let err = compute_advertised(&l, 0, "pod", None, &nodes, None).unwrap_err();
        assert!(matches!(
            err,
            AdvertisedError::PodNotScheduled { broker: 0 }
        ));
    }

    #[test]
    fn nodeport_resolves_external_ip_from_node() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".into(),
            Node {
                status: Some(NodeStatus {
                    addresses: Some(vec![
                        NodeAddress {
                            type_: "InternalIP".into(),
                            address: "10.0.0.1".into(),
                        },
                        NodeAddress {
                            type_: "ExternalIP".into(),
                            address: "1.2.3.4".into(),
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-ext-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094,
                    node_port: Some(32100),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert_eq!(
            a,
            AdvertisedAddress {
                host: "1.2.3.4".into(),
                port: 32100
            }
        );
    }

    #[test]
    fn nodeport_falls_back_to_internal_ip() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".into(),
            Node {
                status: Some(NodeStatus {
                    addresses: Some(vec![NodeAddress {
                        type_: "InternalIP".into(),
                        address: "10.0.0.1".into(),
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-ext-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094,
                    node_port: Some(32100),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert_eq!(a.host, "10.0.0.1");
    }

    #[test]
    fn nodeport_pending_when_service_has_no_nodeport() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert(
            "n1".into(),
            Node {
                status: Some(NodeStatus {
                    addresses: Some(vec![NodeAddress {
                        type_: "InternalIP".into(),
                        address: "10.0.0.1".into(),
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-ext-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094,
                    node_port: None,
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap_err();
        assert!(matches!(err, AdvertisedError::NodePortNotAllocated { .. }));
    }

    #[test]
    fn loadbalancer_resolves_hostname() {
        let l = loadbalancer("lb", 9094);
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-lb-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec::default()),
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        hostname: Some("lb.example.com".into()),
                        ip: None,
                        ip_mode: None,
                        ports: None,
                    }]),
                }),
                ..Default::default()
            }),
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert_eq!(
            a,
            AdvertisedAddress {
                host: "lb.example.com".into(),
                port: 9094
            }
        );
    }

    #[test]
    fn loadbalancer_pending_when_status_missing() {
        let l = loadbalancer("lb", 9094);
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-lb-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec::default()),
            status: None,
        };
        let err = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap_err();
        assert!(matches!(err, AdvertisedError::LoadBalancerPending { .. }));
    }

    #[test]
    fn override_advertised_host_wins() {
        let mut l = nodeport("ext", 9094);
        l.configuration = Some(crate::crd::ListenerConfiguration {
            bootstrap: None,
            brokers: vec![crate::crd::BrokerOverride {
                broker: 0,
                advertised_host: Some("public.host".into()),
                ..Default::default()
            }],
        });
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta {
                name: Some("demo-ext-0".into()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094,
                    node_port: Some(32100),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", None, &nodes, Some(&svc)).unwrap();
        assert_eq!(a.host, "public.host");
        assert_eq!(a.port, 32100);
    }
}

/// Slice 30: inputs to render the broker config-file's TLS block for a
/// single broker. The operator builds this once per reconcile and feeds
/// it into every per-broker TOML — only the leaf cert paths differ per
/// broker (the cert files are addressed by broker id inside the same
/// mount).
#[derive(Debug, Clone)]
pub struct BrokerTlsRender {
    /// e.g. `"Ssl"` or `"SaslSsl"`. Written as the
    /// `controller_listener_protocol = "<v>"` line.
    pub controller_listener_protocol: String,
    /// Path to the broker's own cert (e.g. `/etc/crabka/broker-tls/0.crt`).
    pub cert_path: String,
    /// Path to the broker's own private key.
    pub key_path: String,
    /// Path to the cluster CA cert used to verify peer client certs.
    pub client_ca_path: String,
    /// `"Required"` for inter-broker mTLS.
    pub client_auth: String,
}

/// Render the complete TOML for one broker (cluster-wide content +
/// this broker's advertised addresses). Deterministic — same input
/// always produces byte-identical output so the slice-21 config-hash
/// is stable.
#[allow(dead_code)]
pub fn render_broker_toml(
    broker_id: i32,
    listeners: &[Listener],
    addresses_per_listener: &std::collections::BTreeMap<String, AdvertisedAddress>,
    inter_broker_listener_name: &str,
    server_properties: &std::collections::BTreeMap<String, String>,
    tls: Option<&BrokerTlsRender>,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "broker_id = {broker_id}");
    let _ = writeln!(out, "log_dir = \"/var/lib/crabka/data\"");
    let _ = writeln!(
        out,
        "inter_broker_listener_name = \"{inter_broker_listener_name}\""
    );

    // Emit top-level scalar TLS fields before any [[listeners]] blocks.
    // TOML requires all top-level keys to appear before array-of-tables
    // headers; a bare key after [[listeners]] would be parsed as belonging
    // to that last array entry rather than the root table.
    if let Some(tls) = tls {
        let _ = writeln!(
            out,
            "controller_listener_protocol = \"{}\"",
            tls.controller_listener_protocol
        );
    }
    out.push('\n');

    for l in listeners {
        let adv = addresses_per_listener
            .get(&l.name)
            .map(|a| format!("{}:{}", a.host, a.port))
            .unwrap_or_default();
        let _ = writeln!(out, "[[listeners]]");
        let _ = writeln!(out, "name = \"{}\"", l.name);
        let _ = writeln!(out, "bind_addr = \"0.0.0.0:{}\"", l.port);
        let _ = writeln!(out, "advertised = \"{adv}\"");
        let _ = writeln!(out, "protocol = \"Plaintext\"");
        out.push('\n');
    }

    if !server_properties.is_empty() {
        let _ = writeln!(out, "[server_properties]");
        for (k, v) in server_properties {
            let _ = writeln!(out, "\"{k}\" = \"{v}\"");
        }
        out.push('\n');
    }

    if let Some(tls) = tls {
        let _ = writeln!(out, "[tls_config]");
        let _ = writeln!(out, "cert_path = \"{}\"", tls.cert_path);
        let _ = writeln!(out, "key_path = \"{}\"", tls.key_path);
        let _ = writeln!(out, "client_ca_path = \"{}\"", tls.client_ca_path);
        let _ = writeln!(out, "client_auth = \"{}\"", tls.client_auth);
    }

    out
}

/// Build the synthesized internal-only listener used when
/// `Kafka.spec.listeners` is empty. Kept here so the operator and
/// tests agree on the bytes.
#[allow(dead_code)]
#[must_use]
pub fn synthesized_default_listener() -> Listener {
    Listener {
        name: "PLAIN".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: false,
        authentication: None,
        configuration: None,
        network_policy_peers: None,
    }
}

#[cfg(test)]
mod toml_rendering_tests {
    use super::*;

    #[test]
    fn renders_minimal_broker_toml_and_round_trips() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc.local".into(),
                port: 9092,
            },
        );
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let toml_str = render_broker_toml(0, &listeners, &addrs, "PLAIN", &props, None);

        // Sanity: parses cleanly with the broker's FileConfig.
        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
        assert_eq!(parsed.broker_id, Some(0));
        assert_eq!(parsed.inter_broker_listener_name.as_deref(), Some("PLAIN"));
        assert_eq!(parsed.listeners.len(), 1);
        assert_eq!(parsed.listeners[0].advertised, "demo-0.svc.local:9092");
    }

    #[test]
    fn deterministic_byte_output() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let l = vec![synthesized_default_listener()];
        let mut p = std::collections::BTreeMap::new();
        p.insert("z.last".into(), "1".into());
        p.insert("a.first".into(), "0".into());

        let t1 = render_broker_toml(0, &l, &addrs, "PLAIN", &p, None);
        let t2 = render_broker_toml(0, &l, &addrs, "PLAIN", &p, None);
        assert_eq!(t1, t2);
        // Sorted property keys (BTreeMap iteration).
        let a_pos = t1.find("a.first").unwrap();
        let z_pos = t1.find("z.last").unwrap();
        assert!(a_pos < z_pos);
    }

    #[test]
    fn server_properties_section_omitted_when_empty() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let t = render_broker_toml(
            0,
            &[synthesized_default_listener()],
            &addrs,
            "PLAIN",
            &std::collections::BTreeMap::new(),
            None,
        );
        assert!(!t.contains("[server_properties]"), "got:\n{t}");
    }

    #[test]
    fn render_with_tls_block_round_trips_with_broker_fileconfig() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc.local".into(),
                port: 9092,
            },
        );
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let tls = BrokerTlsRender {
            controller_listener_protocol: "Ssl".into(),
            cert_path: "/etc/crabka/broker-tls/0.crt".into(),
            key_path: "/etc/crabka/broker-tls/0.key".into(),
            client_ca_path: "/etc/crabka/cluster-ca/ca.crt".into(),
            client_auth: "Required".into(),
        };
        let toml_str = render_broker_toml(0, &listeners, &addrs, "PLAIN", &props, Some(&tls));

        let parsed: crabka_broker::file_config::FileConfig =
            toml::from_str(&toml_str).expect("rendered TOML must parse with broker FileConfig");
        assert_eq!(
            parsed.controller_listener_protocol,
            Some(crabka_security::ListenerProtocol::Ssl)
        );
        let parsed_tls = parsed.tls_config.expect("tls_config emitted");
        assert_eq!(
            parsed_tls.cert_path,
            std::path::PathBuf::from("/etc/crabka/broker-tls/0.crt")
        );
    }

    #[test]
    fn render_without_tls_omits_tls_block() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "h".into(),
                port: 9092,
            },
        );
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let toml_str = render_broker_toml(0, &listeners, &addrs, "PLAIN", &props, None);
        assert!(!toml_str.contains("[tls_config]"));
        assert!(!toml_str.contains("controller_listener_protocol"));
    }
}

/// Deterministic serialization of `spec.listeners` intent. Empty
/// (or absent) listeners produce the empty string so a cluster with
/// no `spec.listeners` set keeps its slice-24 hash on upgrade.
#[allow(dead_code)]
pub fn canonical_listener_intent(
    listeners: &[Listener],
    inter_broker_listener_name: Option<&str>,
) -> String {
    use std::fmt::Write as _;
    if listeners.is_empty() {
        return String::new();
    }
    let mut s = String::new();
    if let Some(name) = inter_broker_listener_name {
        let _ = writeln!(s, "inter_broker={name}");
    }
    for l in listeners {
        let _ = writeln!(
            s,
            "listener:name={},port={},type={:?},tls={}",
            l.name, l.port, l.type_, l.tls
        );
        if let Some(cfg) = &l.configuration {
            if let Some(b) = &cfg.bootstrap {
                if let Some(np) = b.node_port {
                    let _ = writeln!(s, "  bootstrap.nodePort={np}");
                }
                if let Some(ip) = &b.load_balancer_ip {
                    let _ = writeln!(s, "  bootstrap.loadBalancerIP={ip}");
                }
            }
            let mut sorted = cfg.brokers.clone();
            sorted.sort_by_key(|o| o.broker);
            for o in &sorted {
                if let Some(h) = &o.advertised_host {
                    let _ = writeln!(s, "  broker{}.advertisedHost={h}", o.broker);
                }
                if let Some(p) = o.advertised_port {
                    let _ = writeln!(s, "  broker{}.advertisedPort={p}", o.broker);
                }
                if let Some(np) = o.node_port {
                    let _ = writeln!(s, "  broker{}.nodePort={np}", o.broker);
                }
                if let Some(ip) = &o.load_balancer_ip {
                    let _ = writeln!(s, "  broker{}.loadBalancerIP={ip}", o.broker);
                }
            }
        }
    }
    s
}

#[cfg(test)]
mod intent_tests {
    use super::*;

    #[test]
    fn empty_listeners_yields_empty_string() {
        assert_eq!(canonical_listener_intent(&[], None), "");
    }

    #[test]
    fn non_empty_listeners_yield_content() {
        let l = vec![synthesized_default_listener()];
        assert!(!canonical_listener_intent(&l, Some("PLAIN")).is_empty());
    }

    #[test]
    fn deterministic() {
        let l = vec![Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: Some(crate::crd::ListenerConfiguration {
                bootstrap: None,
                brokers: vec![
                    crate::crd::BrokerOverride {
                        broker: 1,
                        advertised_host: Some("h1".into()),
                        ..Default::default()
                    },
                    crate::crd::BrokerOverride {
                        broker: 0,
                        advertised_host: Some("h0".into()),
                        ..Default::default()
                    },
                ],
            }),
            network_policy_peers: None,
        }];
        let a = canonical_listener_intent(&l, Some("PLAIN"));
        let b = canonical_listener_intent(&l, Some("PLAIN"));
        assert_eq!(a, b);
        // Sorted by broker id.
        let h0 = a.find("broker0.advertisedHost").unwrap();
        let h1 = a.find("broker1.advertisedHost").unwrap();
        assert!(h0 < h1);
    }
}
