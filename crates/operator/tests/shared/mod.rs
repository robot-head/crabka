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

pub mod fake_admin;
pub mod fake_rebalancer;

use std::sync::Arc;
use std::sync::Mutex;

use crabka_operator::config::OperatorConfig;
use crabka_operator::context::Context;
use crabka_operator::telemetry::new_registry;
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
/// `volumeClaimTemplates` injected into the spec. Slice-24 monotonic-
/// storage validation reads the `data` PVC template's `size` and
/// `storageClassName` off the pre-apply GET response, so the shrink-
/// rejection path needs a way to seed those fields.
///
/// `storage = None` produces an STS body with no `volumeClaimTemplates`
/// (slice-19/20 shape — the pod-template `emptyDir` volume is implied
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
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": { "kafkaVersion": "0.1.1" },
        "status": { "conditions": [] }
    })
}

/// Build an `OperatorConfig` with the slice-19/20 fixture defaults.
pub fn op_config(namespace: &str) -> OperatorConfig {
    OperatorConfig {
        watch_namespaces: vec![],
        operator_namespace: namespace.into(),
        lease_name: "l".into(),
        pod_name: "p".into(),
        health_addr: "0.0.0.0:0".parse().unwrap(),
        log_filter: "info".into(),
        default_broker_image: None,
    }
}

/// Build a `Context` wired to the supplied mock client. Mirrors the
/// slice-19 fixture used by `tests/reconcile.rs`.
pub fn fixture_ctx(client: kube::Client, namespace: &str) -> Context {
    Context::new(
        client,
        op_config(namespace),
        Arc::new(AsyncMutex::new(new_registry())),
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
