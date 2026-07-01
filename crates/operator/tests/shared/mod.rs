//! Shared test harness for the operator's integration tests.
//!
//! Each integration test file (`reconcile_kafka.rs`, `reconcile_pool.rs`)
//! includes this module via `#[path = "shared/mod.rs"] mod shared;`.
//!
//! The harness wires a `tower::Service` mock that:
//!   - matches incoming requests against an ordered list of `MockRule`s
//!     (FIFO: first matching rule wins, and is consumed),
//!   - captures every observed request so the test body can assert on
//!     methods, URIs, and bodies, and
//!   - falls through to a 404 when no rule matches (which fails the test
//!     by surfacing an unexpected request).

#![allow(dead_code)]

use assert2::assert;
pub mod fake_admin;
pub mod fake_rebalancer;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use crabka_operator::config::OperatorConfig;
use crabka_operator::context::Context;
use crabka_operator::crd::{KafkaNodePool, KafkaNodePoolSpec, NodeRole};
use crabka_operator::telemetry::new_registry_with_metrics;
use http::{Method, Request, Response};
use http_body_util::BodyExt as _;
use hyper::body::Bytes;
use kube::Client;
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceBuilder;
use tower::service_fn;

/// One preloaded mock response. Matched on `(method, path_substr)` against
/// the incoming request URI. Substring match is sufficient because kube's
/// generated paths are deterministic and unambiguous.
pub struct MockRule {
    pub method: Method,
    pub path_substr: String,
    pub response: Response<Vec<u8>>,
}

/// Shared mock state: an ordered queue of rules (FIFO consumption) and the
/// list of every request observed (regardless of whether a rule matched).
pub struct MockState {
    pub rules: Mutex<Vec<MockRule>>,
    pub observed: Mutex<Vec<Request<Bytes>>>,
}

impl MockState {
    pub fn new(rules: Vec<MockRule>) -> Arc<Self> {
        Arc::new(Self {
            rules: Mutex::new(rules),
            observed: Mutex::new(Vec::new()),
        })
    }

    pub fn take_observed(&self) -> Vec<Request<Bytes>> {
        std::mem::take(&mut *self.observed.lock().unwrap())
    }

    pub fn remaining_rules(&self) -> usize {
        self.rules.lock().unwrap().len()
    }
}

/// Build a kube `Client` whose underlying transport is the FIFO rule
/// matcher described above. Each call records the request bytes before
/// returning the canned response.
pub fn mock_client(state: &Arc<MockState>, default_ns: &str) -> Client {
    let state_for_svc = state.clone();
    let svc = ServiceBuilder::new().service(service_fn(move |req: Request<kube::client::Body>| {
        let state = state_for_svc.clone();
        async move {
            let (parts, body) = req.into_parts();
            let bytes = body.collect().await.unwrap().to_bytes();
            let captured = Request::from_parts(parts.clone(), bytes);
            state.observed.lock().unwrap().push(captured);

            // FIFO: walk the rule list, take the first match.
            let response = {
                let mut rules = state.rules.lock().unwrap();
                let uri_str = parts.uri.to_string();
                let pos = rules
                    .iter()
                    .position(|r| r.method == parts.method && uri_str.contains(&r.path_substr));
                pos.map(|i| rules.remove(i)).map(|r| r.response)
            };

            let response = response.unwrap_or_else(|| {
                Response::builder()
                    .status(404)
                    .header("content-type", "application/json")
                    .body(not_found_body("unexpected"))
                    .expect("404 response builds")
            });

            let (rp, rb) = response.into_parts();
            Ok::<_, kube::Error>(Response::from_parts(rp, kube::client::Body::from(rb)))
        }
    }));
    Client::new(svc, default_ns)
}

/// Build the apimachinery `Status` body kube-rs parses to recognize a
/// 404. The `code` field is what kube uses to construct `kube::Error::Api`.
pub fn not_found_body(message: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "code": 404,
        "reason": "NotFound",
        "message": message,
    }))
    .expect("status body serializes")
}

pub fn json_response(status: u16, body: &serde_json::Value) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(body).expect("body serializes"))
        .expect("response builds")
}

/// JSON body shaped like a `core/v1/Secret` containing a fake clusterId.
/// Returned for the `POST secrets` step so kube-rs can deserialize the
/// create response.
pub fn fake_secret_body(name: &str, namespace: &str, cluster_id: &str) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(cluster_id.as_bytes());
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "secret-uid" },
        "type": "Opaque",
        "data": { "clusterId": b64 },
    })
}

/// JSON body shaped like an `apps/v1/StatefulSet`. `ready_replicas: None`
/// produces a status without that field; reconcile interprets that as
/// "0 ready".
pub fn fake_sts_body(
    name: &str,
    namespace: &str,
    replicas: i32,
    ready_replicas: Option<i32>,
) -> serde_json::Value {
    fake_sts_body_with_storage(name, namespace, replicas, ready_replicas, None)
}

/// JSON body shaped like an `apps/v1/StatefulSet`, with optional
/// `volumeClaimTemplates` injected into the spec. Monotonic-
/// storage validation reads the `data` PVC template's `size` and
/// `storageClassName` off the pre-apply GET response, so the shrink-
/// rejection path needs a way to seed those fields.
///
/// `storage = None` produces an STS body with no `volumeClaimTemplates`
/// (the pod-template `emptyDir` volume is implied
/// by the absence of a template). `Some((size, class))` embeds:
///
/// ```yaml
/// volumeClaimTemplates:
///   - metadata: { name: "data" }
///     spec:
///       accessModes: [ReadWriteOnce]
///       resources: { requests: { storage: <size> } }
///       storageClassName: <class>   # omitted when class is None
/// ```
pub fn fake_sts_body_with_storage(
    name: &str,
    namespace: &str,
    replicas: i32,
    ready_replicas: Option<i32>,
    storage: Option<(&str, Option<&str>)>,
) -> serde_json::Value {
    let mut status = serde_json::Map::new();
    status.insert("replicas".into(), serde_json::Value::from(replicas));
    if let Some(rr) = ready_replicas {
        status.insert("readyReplicas".into(), serde_json::Value::from(rr));
    }
    let mut spec = serde_json::json!({
        "serviceName": format!("{name}-headless"),
        "replicas": replicas,
        "selector": { "matchLabels": {} },
        "template": {
            "metadata": { "labels": {} },
            "spec": { "containers": [] }
        }
    });
    if let Some((size, class)) = storage {
        let mut pvc = serde_json::json!({
            "metadata": { "name": "data" },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": size } }
            }
        });
        if let Some(c) = class {
            pvc["spec"]["storageClassName"] = serde_json::Value::String(c.into());
        }
        spec["volumeClaimTemplates"] = serde_json::json!([pvc]);
    }
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": { "name": name, "namespace": namespace, "uid": "sts-uid" },
        "spec": spec,
        "status": serde_json::Value::Object(status),
    })
}

/// Faked apply-patch response that echoes a minimal Service object.
pub fn fake_service_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": name, "namespace": namespace, "uid": "svc-uid" },
        "spec": { "clusterIP": "None", "ports": [], "selector": {} }
    })
}

pub fn fake_configmap_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": name, "namespace": namespace, "uid": "cm-uid" },
        "data": {}
    })
}

/// Faked Kafka status PATCH response — kube only needs the body to
/// deserialize back into a `Kafka`, so we echo a minimal valid one.
pub fn fake_kafka_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": { "kafkaVersion": "0.1.1" },
        "status": { "conditions": [] }
    })
}

/// Faked `KafkaTopic` body. kube-rs requires the body deserialize back
/// into a `KafkaTopic`, so we echo a minimal-but-complete one.
pub fn fake_topic_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaTopic",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": "topic-uid",
            "generation": 1,
            "finalizers": ["crabka.io/topic-finalizer"],
        },
        "spec": {
            "partitions": 3,
            "replicas": 1,
            "preserveTopic": false,
        },
        "status": {
            "conditions": [],
        }
    })
}

/// Faked `KafkaRebalance` body. kube-rs requires PATCH responses
/// deserialize back into a `KafkaRebalance`, so we echo a minimal one.
pub fn fake_rebalance_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaRebalance",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": "rebalance-uid",
            "generation": 1,
        },
        "spec": {},
        "status": { "conditions": [] }
    })
}

/// Faked `KafkaNodePool` body (used as the GET response for pool/status
/// PATCH responses). kube-rs requires the body deserialize back into a
/// `KafkaNodePool`.
pub fn fake_pool_body(name: &str, namespace: &str, parent: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaNodePool",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": "pool-uid",
            "labels": { "crabka.io/cluster": parent }
        },
        "spec": {
            "roles": ["Controller", "Broker"],
            "replicas": 1,
            "nodeIdStart": 0
        },
        "status": { "conditions": [] }
    })
}

/// JSON body shaped like a `KafkaNodePoolList`. Used as the GET response
/// for the list-by-label call the Kafka reconciler issues.
pub fn fake_pool_list_body(items: &[serde_json::Value]) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaNodePoolList",
        "metadata": { "resourceVersion": "1" },
        "items": items,
    })
}

/// One pool item to embed in a `KafkaNodePoolList`. `replicas` /
/// `ready_replicas` set the status fields used by the rollup.
pub fn fake_pool_list_item(
    name: &str,
    namespace: &str,
    parent: &str,
    replicas: i32,
    ready_replicas: i32,
) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaNodePool",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": format!("{name}-uid"),
            "labels": { "crabka.io/cluster": parent }
        },
        "spec": {
            "roles": ["Controller", "Broker"],
            "replicas": 1,
            "nodeIdStart": 0
        },
        "status": {
            "conditions": [],
            "replicas": replicas,
            "readyReplicas": ready_replicas
        }
    })
}

/// JSON body shaped like the parent Kafka resource, returned by the
/// pool reconciler's GET kafkas/<parent> step.
pub fn fake_parent_kafka_body(name: &str, namespace: &str) -> serde_json::Value {
    // A parent that the Kafka controller has already reconciled carries a
    // cleared version model: `KafkaVersionValid=True` + a finalized
    // `status.metadataVersion`. The pool reconciler gates pod creation on
    // this (see `kafka_node_pool::version_gate`), so the fixture must look
    // like a validated cluster for the happy-path STS-apply tests to fire.
    //
    // It also exposes a realistic dual-listener topology: a plaintext `PLAIN`
    // listener on 9092 plus a TLS + `authentication: tls` (mTLS) internal
    // listener `tls-internal` on 9093. The gateway reconciler resolves its
    // broker mTLS bootstrap + SNI from the SECURED listener (not the
    // plaintext :9092). Both `spec.listeners` (for the tls/auth predicate)
    // and `status.listeners` (for the resolved bootstrap host:port) carry it.
    let tls_bootstrap = format!("{name}-broker-headless.{namespace}.svc.cluster.local:9093");
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": {
            "kafkaVersion": "0.1.1",
            "listeners": [
                { "name": "PLAIN", "port": 9092, "type": "internal", "tls": false },
                {
                    "name": "tls-internal",
                    "port": 9093,
                    "type": "internal",
                    "tls": true,
                    "authentication": { "type": "tls" }
                }
            ]
        },
        "status": {
            "conditions": [{
                "type": "KafkaVersionValid",
                "status": "True",
                "reason": "Valid",
                "message": "kafkaVersion 0.1.1 metadata.version 0.1",
                "lastTransitionTime": "2026-05-22T00:00:00Z"
            }],
            "metadataVersion": "0.1",
            "listeners": [
                {
                    "name": "PLAIN",
                    "type": "internal",
                    "bootstrapServers": format!("{name}-broker-headless.{namespace}.svc.cluster.local:9092")
                },
                {
                    "name": "tls-internal",
                    "type": "internal",
                    "bootstrapServers": tls_bootstrap
                }
            ]
        }
    })
}

/// Build an `OperatorConfig` with the fixture defaults.
pub fn op_config(namespace: &str) -> OperatorConfig {
    OperatorConfig {
        watch_namespaces: vec![],
        operator_namespace: namespace.into(),
        lease_name: "l".into(),
        pod_name: "p".into(),
        health_addr: "0.0.0.0:0".parse().unwrap(),
        log_filter: "info".into(),
        default_broker_image: None,
        default_gateway_image: None,
        default_schema_registry_image: None,
    }
}

/// Build a `Context` wired to the supplied mock client. Mirrors the
/// fixture used by `tests/reconcile.rs`.
pub fn fixture_ctx(client: kube::Client, namespace: &str) -> Context {
    let (registry, metrics) = new_registry_with_metrics();
    Context::new(
        client,
        op_config(namespace),
        Arc::new(AsyncMutex::new(registry)),
        metrics,
    )
}

// ---------------------------------------------------------------------------
// CA / keystore helpers shared across reconcile_ca, reconcile_inter_broker_mtls,
// and reconcile_listener_auth.
// ---------------------------------------------------------------------------

/// Minimal CA Secret body (empty data). Returned by PATCH responses for both
/// the cluster-CA and clients-CA Secret pairs.
pub fn fake_ca_secret(sname: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": sname, "namespace": namespace, "uid": "ca-uid" },
        "type": "Opaque",
        "data": {}
    })
}

/// Minimal broker keystore Secret body (empty data). Returned by PATCH
/// responses for the `<cluster>-kafka-brokers` Secret.
pub fn fake_keystore_secret(sname: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": sname, "namespace": namespace, "uid": "ks-uid" },
        "type": "Opaque",
        "data": {}
    })
}

/// Full happy-path FIFO rule list for a Kafka reconcile of cluster `name` in
/// `namespace`. Covers headless-service, cluster-id, cluster-CA, clients-CA,
/// pool-list, broker-keystore, config-map, pool owner-ref, and status PATCH.
#[allow(clippy::too_many_lines)]
pub fn happy_path_rules(
    name: &str,
    namespace: &str,
    pool_items: &[serde_json::Value],
) -> Vec<MockRule> {
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let clients_ca_key = format!("{name}-clients-ca");
    let clients_ca_cert = format!("{name}-clients-ca-cert");
    let keystore_name = format!("{name}-kafka-brokers");

    let mut rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, namespace)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{secret_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{namespace}/secrets"),
            response: json_response(
                201,
                &fake_secret_body(
                    &secret_name,
                    namespace,
                    "00000000-0000-0000-0000-000000000000",
                ),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_key, namespace)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_cert, namespace)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_key, namespace)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_cert, namespace)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{namespace}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(pool_items)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{keystore_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &fake_keystore_secret(&keystore_name, namespace)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, namespace)),
        },
    ];

    for item in pool_items {
        let pool_name = item["metadata"]["name"]
            .as_str()
            .expect("pool item has metadata.name");
        rules.push(MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}?"),
            response: json_response(200, &fake_pool_body(pool_name, namespace, name)),
        });
    }
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkas/{name}/status"),
        response: json_response(200, &fake_kafka_body(name, namespace)),
    });
    rules
}

/// Build a `Context` + `MockState` pair wired to the FIFO mock transport.
pub fn build_ctx(
    namespace: &str,
    rules: Vec<MockRule>,
) -> (Arc<crabka_operator::context::Context>, Arc<MockState>) {
    let state = MockState::new(rules);
    let client = mock_client(&state, namespace);
    (Arc::new(fixture_ctx(client, namespace)), state)
}

// ---------------------------------------------------------------------------
// OAuth listener-secret helpers shared across reconcile_oauth_introspection,
// reconcile_oauth_trust, reconcile_listener_oauth, and reconcile_listener_auth.
// ---------------------------------------------------------------------------

/// Build a rule for `GET /secrets/<name>` returning a 404.
pub fn rule_get_secret_404(name: &str) -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: format!("/secrets/{name}"),
        response: Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(not_found_body("not found"))
            .expect("404 builds"),
    }
}

/// Build a rule for `GET /secrets/<name>` returning the supplied body.
pub fn rule_get_secret(name: &str, body: &serde_json::Value) -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: format!("/secrets/{name}"),
        response: json_response(200, body),
    }
}

/// Trim down `happy_path_rules` for the OAuth failure-path tests: when an
/// OAuth Secret-touching step fails, the reconciler short-circuits to a
/// status PATCH on the `Kafka` CR before upserting per-broker objects.
/// Drop the rules that fire *after* that point — broker keystore,
/// broker-config `ConfigMap`, and the final aggregated status PATCH — so an
/// unconsumed rule doesn't mask the failure assertion, then add the GET +
/// PATCH status pair that `patch_status_with_condition` issues. The pool
/// LIST is retained: it fires *before* the Secret-touching code for
/// CA-rotation convergence (and the empty pool-items list adds no per-pool
/// PATCH rules anyway).
pub fn rules_for_failure_path(name: &str, namespace: &str) -> Vec<MockRule> {
    let mut rules = happy_path_rules(name, namespace, &[]);
    rules.retain(|r| {
        !r.path_substr.contains("-kafka-brokers")
            && !r.path_substr.contains("/configmaps/")
            && !r.path_substr.contains(&format!("/kafkas/{name}/status"))
    });
    // The failure path calls `patch_status_with_condition`, which does
    // GET status + PATCH status.
    rules.push(MockRule {
        method: Method::GET,
        path_substr: format!("/kafkas/{name}/status"),
        response: json_response(200, &fake_kafka_body(name, namespace)),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkas/{name}/status"),
        response: json_response(200, &fake_kafka_body(name, namespace)),
    });
    rules
}

/// Find the `Ready=False` condition in the status PATCH body and assert
/// its `reason` matches. The OAuth Secret-touching failure paths patch
/// the `Ready` condition (not `ListenersValid` — per-listener validation
/// already succeeded; the failure is in the Secret-touching code that
/// runs *after* validation).
pub fn assert_ready_false_with_reason(
    observed: &[http::Request<hyper::body::Bytes>],
    cluster: &str,
    expected_reason: &str,
) {
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkas/{cluster}/status"))
        })
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");
    let ready = conds
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap_or_else(|| panic!("Ready condition present; body = {body}"));
    assert!(ready["status"] == "False", "body = {body}");
    assert!(ready["reason"] == expected_reason, "body = {body}");
}

/// Extract the `broker-0.toml` string from the `ConfigMap` PATCH captured
/// in `observed`. Panics with a diagnostic message if the PATCH (or the
/// key) is missing.
pub fn extract_broker0_toml(
    observed: &[http::Request<hyper::body::Bytes>],
    cluster: &str,
) -> String {
    let cm_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/configmaps/{cluster}-broker-config"))
        })
        .unwrap_or_else(|| panic!("ConfigMap PATCH not found for cluster {cluster}"));

    let body: serde_json::Value =
        serde_json::from_slice(cm_patch.body()).expect("ConfigMap PATCH body is JSON");
    let data = body
        .get("data")
        .and_then(|d| d.as_object())
        .unwrap_or_else(|| panic!("ConfigMap PATCH has no data object; body = {body}"));

    data.get("broker-0.toml")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            panic!(
                "broker-0.toml missing; data keys = {:?}",
                data.keys().collect::<Vec<_>>()
            )
        })
        .to_string()
}

// ---------------------------------------------------------------------------
// Pool-reconcile fixtures shared across reconcile_oauth_introspection and
// reconcile_oauth_trust. (Other test files define same-named helpers with
// different signatures; those are intentionally distinct and not hoisted.)
// ---------------------------------------------------------------------------

/// Build a `KafkaNodePool` CR labelled with its parent cluster.
pub fn pool_cr(name: &str, namespace: &str, parent: &str, replicas: i32) -> KafkaNodePool {
    let mut p = KafkaNodePool::new(
        name,
        KafkaNodePoolSpec {
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            replicas,
            node_id_start: 0,
            image: None,
            resources: None,
            template: None,
            storage: None,
        },
    );
    p.metadata.namespace = Some(namespace.into());
    p.metadata.uid = Some("pool-uid".into());
    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/cluster".into(), parent.into());
    p.metadata.labels = Some(labels);
    p
}

/// Build the FIFO rule sequence the pool reconciler needs:
///   1. GET kafkas/<parent>            → `parent_body`
///   2. GET statefulsets/<parent>-<pool> → 404 (first reconcile)
///   3. PATCH statefulsets/<parent>-<pool> (SSA)
///   4. GET statefulsets/<parent>-<pool> (post-apply status read)
///   5. PATCH kafkanodepools/<pool>/status
pub fn pool_reconcile_rules(
    parent: &str,
    pool: &str,
    namespace: &str,
    parent_body: &serde_json::Value,
) -> Vec<MockRule> {
    let sts_name = format!("{parent}-{pool}");
    vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, parent_body),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("first reconcile, no live STS"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, Some(1))),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, Some(1))),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool}/status"),
            response: json_response(200, &fake_pool_body(pool, namespace, parent)),
        },
    ]
}

// ---------------------------------------------------------------------------
// Gateway reconcile helpers
// ---------------------------------------------------------------------------

/// JSON body shaped like an `apps/v1/Deployment`.
/// `ready_replicas = None` → no `readyReplicas` in status (= 0 ready).
pub fn fake_deployment_body(
    name: &str,
    namespace: &str,
    ready_replicas: Option<i32>,
) -> serde_json::Value {
    let mut status = serde_json::json!({ "replicas": 1 });
    if let Some(rr) = ready_replicas {
        status["readyReplicas"] = serde_json::Value::from(rr);
    }
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": name, "namespace": namespace, "uid": "dep-uid" },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": {} },
            "template": {
                "metadata": { "labels": {} },
                "spec": { "containers": [] }
            }
        },
        "status": status
    })
}

/// JSON body shaped like a `KafkaGrpcGateway` CR.
/// Carries `metadata.labels["crabka.io/cluster"] = kafka_name` and a `uid`.
pub fn fake_gateway_body(name: &str, namespace: &str, kafka_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaGrpcGateway",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": "gw-uid",
            "generation": 1,
            "labels": { "crabka.io/cluster": kafka_name }
        },
        "spec": {
            "webhooks": [],
            "outboundSubscriptions": [],
            "allowedTargets": []
        },
        "status": { "conditions": [] }
    })
}

/// Build a cluster-CA key Secret (`<kafka>-cluster-ca`, key `ca.key`)
/// containing a real ECDSA CA private key PEM.
pub fn fake_cluster_ca_key_secret(
    kafka_name: &str,
    namespace: &str,
    key_pem: &str,
) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(key_pem.as_bytes());
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": format!("{kafka_name}-cluster-ca"),
            "namespace": namespace,
            "uid": "cluster-ca-key-uid"
        },
        "type": "Opaque",
        "data": { "ca.key": b64 }
    })
}

/// Build a cluster-CA cert Secret (`<kafka>-cluster-ca-cert`, key `ca.crt`)
/// containing a real X.509 CA cert PEM.
pub fn fake_cluster_ca_cert_secret(
    kafka_name: &str,
    namespace: &str,
    cert_pem: &str,
) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(cert_pem.as_bytes());
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": format!("{kafka_name}-cluster-ca-cert"),
            "namespace": namespace,
            "uid": "cluster-ca-cert-uid"
        },
        "type": "Opaque",
        "data": { "ca.crt": b64 }
    })
}

/// Build a `<gw>-broker` `KafkaUser` Secret (keys `user.crt` / `user.key` /
/// `ca.crt`) with placeholder PEM bytes. Returned on the post-SSA GET that
/// the gateway reconciler uses to check that the cert has been issued.
pub fn fake_broker_user_secret(gw_name: &str, namespace: &str) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    let placeholder = b64.encode(b"placeholder");
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": format!("{gw_name}-broker"),
            "namespace": namespace,
            "uid": "broker-secret-uid"
        },
        "type": "Opaque",
        "data": {
            "user.crt": placeholder,
            "user.key": placeholder,
            "ca.crt": placeholder
        }
    })
}

/// Build a minimal `<gw>-serving` Secret body (keys `tls.crt` / `tls.key`).
/// Returned by the PATCH apply call for the serving-cert Secret.
pub fn fake_serving_secret(gw_name: &str, namespace: &str) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    let placeholder = b64.encode(b"placeholder");
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": format!("{gw_name}-serving"),
            "namespace": namespace,
            "uid": "serving-secret-uid"
        },
        "type": "Opaque",
        "data": { "tls.crt": placeholder, "tls.key": placeholder }
    })
}

/// Build a minimal `<gw>-config` Secret body (keys `webhooks.toml` /
/// `outbound.toml`). Returned by the PATCH apply for the config Secret.
pub fn fake_config_secret(gw_name: &str, namespace: &str) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    let webhooks = b64.encode(b"[endpoints]\n");
    let outbound = b64.encode(b"[subscriptions]\n");
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": format!("{gw_name}-config"),
            "namespace": namespace,
            "uid": "config-secret-uid"
        },
        "type": "Opaque",
        "data": { "webhooks.toml": webhooks, "outbound.toml": outbound }
    })
}

/// Build a `KafkaUser` minimal body.
pub fn fake_kafkauser_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaUser",
        "metadata": { "name": name, "namespace": namespace, "uid": "user-uid", "generation": 1 },
        "spec": { "authentication": { "type": "tls" } },
        "status": { "conditions": [] }
    })
}
