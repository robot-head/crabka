//! Integration tests for `ingress` and `route` external listeners.
//!
//! These tests drive `reconcile` against the FIFO mock transport. See
//! `shared/mod.rs`. They assert that the operator renders the `ClusterIP`
//! backend Services and the `Ingress`/`Route` objects. They also assert that
//! the operator renders a `ConfigMap` whose advertised address is
//! `<host>:443`, and a `ListenersReady=True` status.

use std::sync::Arc;

use assert2::{assert, check};
use crabka_operator::{
    controller::kafka::reconcile,
    crd::{
        BootstrapConfig, BrokerOverride, Kafka, KafkaSpec, Listener, ListenerConfiguration,
        ListenerType,
    },
};
use http::Method;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, build_ctx, fake_pool_list_item, fake_service_body, happy_path_rules, json_response,
};

// ── helpers ──────────────────────────────────────────────────────────────────

fn kafka_cr(name: &str, namespace: &str, listeners: Vec<Listener>) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners,
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy: None,
            cluster_ca: None,
            clients_ca: None,
            logging: None,
            delegation_token: None,
            authorization: None,
            tiered_storage: None,
            inter_broker_kerberos: None,
            krb5_conf_secret_ref: None,
            tracing: None,
            broker_tuning: None,
            gres_registry: None,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

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

fn external(name: &str, port: i32, type_: ListenerType, broker0_host: &str) -> Listener {
    Listener {
        name: name.into(),
        port,
        type_,
        tls: true,
        authentication: None,
        configuration: Some(ListenerConfiguration {
            bootstrap: Some(BootstrapConfig {
                host: Some("bootstrap.kafka.example.com".into()),
                ..Default::default()
            }),
            brokers: vec![BrokerOverride {
                broker: 0,
                host: Some(broker0_host.into()),
                ..Default::default()
            }],
            ingress_class: Some("nginx".into()),
        }),
        network_policy_peers: None,
    }
}

fn fake_ingress_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": { "name": name, "namespace": namespace, "uid": "ing-uid" },
        "spec": { "rules": [] }
    })
}

fn fake_route_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "route.openshift.io/v1",
        "kind": "Route",
        "metadata": { "name": name, "namespace": namespace, "uid": "route-uid" },
        "spec": {}
    })
}

fn body_of(
    observed: &[http::Request<hyper::body::Bytes>],
    method: &Method,
    substr: &str,
) -> serde_json::Value {
    let req = observed
        .iter()
        .find(|r| r.method() == method && r.uri().to_string().contains(substr))
        .unwrap_or_else(|| {
            panic!(
                "no {method} request matching {substr}; observed: {:?}",
                observed
                    .iter()
                    .map(|r| format!("{} {}", r.method(), r.uri()))
                    .collect::<Vec<_>>()
            )
        });
    serde_json::from_slice(req.body()).expect("request body is JSON")
}

fn broker0_toml(observed: &[http::Request<hyper::body::Bytes>], cluster: &str) -> String {
    let body = body_of(
        observed,
        &Method::PATCH,
        &format!("/configmaps/{cluster}-broker-config"),
    );
    body["data"]["broker-0.toml"]
        .as_str()
        .unwrap_or_else(|| panic!("broker-0.toml missing; body = {body}"))
        .to_string()
}

// ── test 1: ingress ──────────────────────────────────────────────────────────

#[tokio::test]
async fn ingress_listener_renders_ingress_objects_and_advertises_443() {
    let ns = "ing1";
    let name = "c1";
    let items = vec![fake_pool_list_item("brokers", ns, name, 1, 1)];
    let mut rules = happy_path_rules(name, ns, &items);

    // apply_external_services: ClusterIP backend Services + Ingress objects.
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/services/{name}-ext-bootstrap"),
        response: json_response(
            200,
            &fake_service_body(&format!("{name}-ext-bootstrap"), ns),
        ),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/services/{name}-ext-0"),
        response: json_response(200, &fake_service_body(&format!("{name}-ext-0"), ns)),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/ingresses/{name}-ext-bootstrap"),
        response: json_response(
            200,
            &fake_ingress_body(&format!("{name}-ext-bootstrap"), ns),
        ),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/ingresses/{name}-ext-0"),
        response: json_response(200, &fake_ingress_body(&format!("{name}-ext-0"), ns)),
    });

    let (ctx, state) = build_ctx(ns, rules);

    let kafka = kafka_cr(
        name,
        ns,
        vec![
            internal("PLAIN", 9092),
            external(
                "ext",
                9094,
                ListenerType::Ingress,
                "broker-0.kafka.example.com",
            ),
        ],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // The per-broker Ingress carries the SNI passthrough annotation, the
    // ingress class, and routes broker-0's hostname to its backend Service.
    let ing = body_of(
        &observed,
        &Method::PATCH,
        &format!("/ingresses/{name}-ext-0"),
    );
    check!(
        ing["metadata"]["annotations"]["nginx.ingress.kubernetes.io/ssl-passthrough"] == "true",
        "ingress = {ing}"
    );
    check!(
        ing["spec"]["ingressClassName"] == "nginx",
        "ingress = {ing}"
    );
    check!(
        ing["spec"]["rules"][0]["host"] == "broker-0.kafka.example.com",
        "ingress = {ing}"
    );
    check!(
        ing["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"]
            == format!("{name}-ext-0"),
        "ingress = {ing}"
    );

    // ConfigMap advertises the ingress host on 443.
    let toml = broker0_toml(&observed, name);
    assert!(
        toml.contains("advertised = \"broker-0.kafka.example.com:443\""),
        "expected ingress advertised on :443;\n{toml}"
    );

    // Status: ListenersReady=True and the bootstrap address is on :443.
    let status = body_of(&observed, &Method::PATCH, &format!("/kafkas/{name}/status"));
    let conds = status["status"]["conditions"].as_array().unwrap();
    let ready = conds
        .iter()
        .find(|c| c["type"] == "ListenersReady")
        .unwrap();
    assert!(ready["status"] == "True", "status = {status}");
    let ext = status["status"]["listeners"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["name"] == "ext")
        .unwrap_or_else(|| panic!("ext listener status missing; status = {status}"));
    assert!(ext["bootstrapServers"] == "bootstrap.kafka.example.com:443");

    assert!(state.remaining_rules() == 0, "all rules consumed");
}

// ── test 2: route ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn route_listener_renders_passthrough_route_objects() {
    let ns = "rt1";
    let name = "c2";
    let items = vec![fake_pool_list_item("brokers", ns, name, 1, 1)];
    let mut rules = happy_path_rules(name, ns, &items);

    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/services/{name}-ext-bootstrap"),
        response: json_response(
            200,
            &fake_service_body(&format!("{name}-ext-bootstrap"), ns),
        ),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/services/{name}-ext-0"),
        response: json_response(200, &fake_service_body(&format!("{name}-ext-0"), ns)),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/routes/{name}-ext-bootstrap"),
        response: json_response(200, &fake_route_body(&format!("{name}-ext-bootstrap"), ns)),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/routes/{name}-ext-0"),
        response: json_response(200, &fake_route_body(&format!("{name}-ext-0"), ns)),
    });

    let (ctx, state) = build_ctx(ns, rules);

    let kafka = kafka_cr(
        name,
        ns,
        vec![
            internal("PLAIN", 9092),
            external(
                "ext",
                9094,
                ListenerType::Route,
                "broker-0.kafka.example.com",
            ),
        ],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    let route = body_of(&observed, &Method::PATCH, &format!("/routes/{name}-ext-0"));
    check!(
        route["spec"]["tls"]["termination"] == "passthrough",
        "route = {route}"
    );
    check!(
        route["spec"]["host"] == "broker-0.kafka.example.com",
        "route = {route}"
    );
    check!(
        route["spec"]["port"]["targetPort"] == 9094,
        "route = {route}"
    );
    check!(
        route["spec"]["to"]["name"] == format!("{name}-ext-0"),
        "route = {route}"
    );

    let toml = broker0_toml(&observed, name);
    assert!(
        toml.contains("advertised = \"broker-0.kafka.example.com:443\""),
        "expected route advertised on :443;\n{toml}"
    );

    assert!(state.remaining_rules() == 0, "all rules consumed");
}

// ── test 3: validation failure ────────────────────────────────────────────────

#[tokio::test]
async fn ingress_without_tls_surfaces_validation_error() {
    let ns = "ing3";
    let name = "c3";
    let items = vec![fake_pool_list_item("brokers", ns, name, 1, 1)];
    let mut rules = happy_path_rules(name, ns, &items);
    // Validation fails before the ConfigMap / keystore are rendered.
    rules.retain(|r| !r.path_substr.contains("/configmaps/"));
    rules.retain(|r| !r.path_substr.contains("-kafka-brokers"));
    let (ctx, state) = build_ctx(ns, rules);

    // ingress listener with tls: false → ListenerIngressRequiresTls.
    let mut bad = external(
        "ext",
        9094,
        ListenerType::Ingress,
        "broker-0.kafka.example.com",
    );
    bad.tls = false;
    let kafka = kafka_cr(name, ns, vec![internal("PLAIN", 9092), bad]);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert!(
        !observed.iter().any(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/configmaps/{name}-broker-config"))
        }),
        "validation failure must not patch the broker-config ConfigMap"
    );

    let status = body_of(&observed, &Method::PATCH, &format!("/kafkas/{name}/status"));
    let conds = status["status"]["conditions"].as_array().unwrap();
    let valid = conds
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap();
    assert!(valid["status"] == "False", "status = {status}");
    assert!(
        valid["reason"] == "ListenerIngressRequiresTls",
        "status = {status}"
    );
}
