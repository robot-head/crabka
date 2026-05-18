//! `Kafka.spec.listeners` schema — Strimzi-shaped.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Listener {
    /// Unique within the cluster. Alphanumeric + `-`, ≤25 chars. Used
    /// as the Kafka listener name; surfaces in `bootstrap.servers`-style
    /// URLs.
    pub name: String,
    /// Container port the broker binds. Unique within the cluster.
    pub port: i32,
    /// Listener type. `internal` is in-cluster; `nodeport` /
    /// `loadbalancer` create external Services; `ingress` / `route` are
    /// accepted by the schema but rejected at reconcile until slice 27.
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// Must be `false` in this slice; reconcile rejects `true` until
    /// Phase 4 (slices 30/31) wires up TLS.
    #[serde(default)]
    pub tls: bool,
    /// Optional listener-type-specific configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<ListenerConfiguration>,
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
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ListenerType {
    #[default]
    Internal,
    Nodeport,
    Loadbalancer,
    Ingress,
    Route,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<BootstrapConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<BrokerOverride>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapConfig {
    /// `nodeport` only: pin the bootstrap `NodePort`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
    /// `loadbalancer` only: pin the bootstrap LB IP.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "loadBalancerIP"
    )]
    pub load_balancer_ip: Option<String>,
    /// `ingress` / `route` only (slice 27): bootstrap hostname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Annotations to add to the bootstrap Service.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    /// Labels to add to the bootstrap Service.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BrokerOverride {
    /// Broker id this override applies to (matches the node id).
    pub broker: i32,
    /// Override the computed advertised host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertised_host: Option<String>,
    /// Override the computed advertised port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertised_port: Option<i32>,
    /// `nodeport` only: pin this broker's `Service.spec.ports[0].nodePort`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
    /// `loadbalancer` only: pin this broker's `Service.spec.loadBalancerIP`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "loadBalancerIP"
    )]
    pub load_balancer_ip: Option<String>,
    /// `ingress` / `route` only (slice 27): per-broker hostname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerStatus {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// `host:port` clients should put in `bootstrap.servers`.
    pub bootstrap_servers: String,
    #[serde(default)]
    pub addresses: Vec<ListenerAddress>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAddress {
    pub host: String,
    pub port: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_listener_round_trips_through_json() {
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            configuration: None,
            network_policy_peers: None,
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(json.contains("\"type\":\"internal\""), "got: {json}");
        assert!(json.contains("\"port\":9092"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn nodeport_with_broker_overrides_round_trips() {
        let l = Listener {
            name: "external".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: false,
            configuration: Some(ListenerConfiguration {
                bootstrap: Some(BootstrapConfig {
                    node_port: Some(32099),
                    ..Default::default()
                }),
                brokers: vec![BrokerOverride {
                    broker: 0,
                    advertised_host: Some("public.host".into()),
                    node_port: Some(32100),
                    ..Default::default()
                }],
            }),
            network_policy_peers: None,
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(
            json.contains("\"advertisedHost\":\"public.host\""),
            "got: {json}"
        );
        assert!(json.contains("\"nodePort\":32100"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn camelcase_wire_shape() {
        let cfg = ListenerConfiguration {
            bootstrap: Some(BootstrapConfig {
                load_balancer_ip: Some("10.0.0.5".into()),
                ..Default::default()
            }),
            brokers: vec![],
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(
            json.contains("\"loadBalancerIP\":\"10.0.0.5\""),
            "got: {json}"
        );
    }

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
        assert!(
            json.contains("\"matchLabels\":{\"role\":\"client\"}"),
            "got: {json}"
        );
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }
}
