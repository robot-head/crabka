//! Mocked-client integration tests for the `Kafka` reconciler.
//!
//! Each test wires a `tower::Service` mock that:
//!   - matches incoming requests against an ordered list of `MockRule`s
//!     (FIFO: first matching rule wins, and is consumed),
//!   - captures every observed request so the test body can assert on
//!     methods, URIs, and bodies, and
//!   - falls through to a 404 when no rule matches (which fails the test
//!     by surfacing an unexpected request).
//!
//! The reconcile request sequence on a fresh Kafka is exactly:
//!   1. PATCH services/<name>-broker-headless (SSA)
//!   2. PATCH configmaps/<name>-broker-config (SSA)
//!   3. GET secrets/<name>-cluster-id          (-> 404)
//!   4. POST secrets                            (-> 201)
//!   5. PATCH statefulsets/<name>-broker       (SSA)
//!   6. GET statefulsets/<name>-broker          (status read)
//!   7. PATCH kafkas/<name>/status              (merge)
//!
//! The validation-failure path issues only step 7.

use std::sync::Arc;
use std::sync::Mutex;

use crabka_operator::config::OperatorConfig;
use crabka_operator::context::Context;
use crabka_operator::controller::kafka::reconcile;
use crabka_operator::crd::{Kafka, KafkaSpec};
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
struct MockRule {
    method: Method,
    path_substr: String,
    response: Response<Vec<u8>>,
}

/// Shared mock state: an ordered queue of rules (FIFO consumption) and the
/// list of every request observed (regardless of whether a rule matched).
struct MockState {
    rules: Mutex<Vec<MockRule>>,
    observed: Mutex<Vec<Request<Bytes>>>,
}

impl MockState {
    fn new(rules: Vec<MockRule>) -> Arc<Self> {
        Arc::new(Self {
            rules: Mutex::new(rules),
            observed: Mutex::new(Vec::new()),
        })
    }

    fn take_observed(&self) -> Vec<Request<Bytes>> {
        std::mem::take(&mut *self.observed.lock().unwrap())
    }

    fn remaining_rules(&self) -> usize {
        self.rules.lock().unwrap().len()
    }
}

/// Build a kube `Client` whose underlying transport is the FIFO rule
/// matcher described above. Each call records the request bytes before
/// returning the canned response.
fn mock_client(state: &Arc<MockState>, default_ns: &str) -> Client {
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
fn not_found_body(message: &str) -> Vec<u8> {
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

fn json_response(status: u16, body: &serde_json::Value) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(body).expect("body serializes"))
        .expect("response builds")
}

/// JSON body shaped like a `core/v1/Secret` containing a fake clusterId.
/// Returned for the `POST secrets` step so kube-rs can deserialize the
/// create response.
fn fake_secret_body(name: &str, namespace: &str, cluster_id: &str) -> serde_json::Value {
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
fn fake_sts_body(
    name: &str,
    namespace: &str,
    replicas: i32,
    ready_replicas: Option<i32>,
) -> serde_json::Value {
    let mut status = serde_json::Map::new();
    status.insert("replicas".into(), serde_json::Value::from(replicas));
    if let Some(rr) = ready_replicas {
        status.insert("readyReplicas".into(), serde_json::Value::from(rr));
    }
    serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": { "name": name, "namespace": namespace, "uid": "sts-uid" },
        "spec": {
            "serviceName": format!("{name}-headless"),
            "replicas": replicas,
            "selector": { "matchLabels": {} },
            "template": {
                "metadata": { "labels": {} },
                "spec": { "containers": [] }
            }
        },
        "status": serde_json::Value::Object(status),
    })
}

/// Faked apply-patch response that echoes a minimal object. kube-rs only
/// requires that the response deserialize to the requested resource type.
fn fake_service_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": name, "namespace": namespace, "uid": "svc-uid" },
        "spec": { "clusterIP": "None", "ports": [], "selector": {} }
    })
}

fn fake_configmap_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": name, "namespace": namespace, "uid": "cm-uid" },
        "data": {}
    })
}

/// Faked Kafka status PATCH response — kube only needs the body to
/// deserialize back into a `Kafka`, so we echo a minimal valid one.
fn fake_kafka_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": { "kafkaVersion": "0.1.1", "replicas": 1 },
        "status": { "conditions": [] }
    })
}

fn op_config(namespace: &str) -> OperatorConfig {
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

fn kafka_cr(name: &str, namespace: &str, replicas: i32) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            replicas,
            image: None,
            resources: None,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Standard rule list for a successful reconcile of `demo` in namespace
/// `y`. Caller controls how the GET statefulsets response reports its
/// status by passing `ready_replicas`.
fn happy_path_rules(name: &str, namespace: &str, ready_replicas: Option<i32>) -> Vec<MockRule> {
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");
    let sts_name = format!("{name}-broker");

    vec![
        // 1. PATCH service
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, namespace)),
        },
        // 2. PATCH configmap
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, namespace)),
        },
        // 3. GET secret -> 404
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{secret_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("secret not found"))
                .expect("404 builds"),
        },
        // 4. POST secret -> 201
        MockRule {
            method: Method::POST,
            // The create endpoint is the collection URL ending in `/secrets`.
            // We also see a `?fieldManager=…` query string; substring match
            // tolerates that.
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
        // 5. PATCH statefulset
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, ready_replicas)),
        },
        // 6. GET statefulset (status read)
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, ready_replicas)),
        },
        // 7. PATCH kafkas/<name>/status
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkas/{name}/status"),
            response: json_response(200, &fake_kafka_body(name, namespace)),
        },
    ]
}

fn build_ctx(name: &str, namespace: &str, rules: Vec<MockRule>) -> (Arc<Context>, Arc<MockState>) {
    let state = MockState::new(rules);
    let client = mock_client(&state, namespace);
    let _ = name;
    let ctx = Context::new(
        client,
        op_config(namespace),
        Arc::new(AsyncMutex::new(new_registry())),
    );
    (Arc::new(ctx), state)
}

#[tokio::test]
async fn reconcile_applies_service_configmap_secret_statefulset_on_create() {
    let (ctx, state) = build_ctx("demo", "y", happy_path_rules("demo", "y", Some(1)));
    let kafka = kafka_cr("demo", "y", 1);

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_eq!(
        observed.len(),
        7,
        "expected 7 requests (4 PATCH + 1 GET secret + 1 POST secret + 1 GET sts), saw {}: {:?}",
        observed.len(),
        observed
            .iter()
            .map(|r| (r.method().clone(), r.uri().to_string()))
            .collect::<Vec<_>>()
    );

    // Sequence-tight checks: the reconciler issues requests in this fixed order.
    let methods_and_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    assert_eq!(methods_and_uris[0].0, Method::PATCH);
    assert!(
        methods_and_uris[0]
            .1
            .contains("/services/demo-broker-headless"),
        "first request should be the service apply: {}",
        methods_and_uris[0].1
    );

    assert_eq!(methods_and_uris[1].0, Method::PATCH);
    assert!(
        methods_and_uris[1]
            .1
            .contains("/configmaps/demo-broker-config"),
        "second request should be the configmap apply: {}",
        methods_and_uris[1].1
    );

    assert_eq!(methods_and_uris[2].0, Method::GET);
    assert!(
        methods_and_uris[2].1.contains("/secrets/demo-cluster-id"),
        "third request should be the secret get: {}",
        methods_and_uris[2].1
    );

    assert_eq!(methods_and_uris[3].0, Method::POST);
    assert!(
        methods_and_uris[3].1.contains("/namespaces/y/secrets"),
        "fourth request should be the secret create: {}",
        methods_and_uris[3].1
    );

    assert_eq!(methods_and_uris[4].0, Method::PATCH);
    assert!(
        methods_and_uris[4].1.contains("/statefulsets/demo-broker"),
        "fifth request should be the statefulset apply: {}",
        methods_and_uris[4].1
    );

    assert_eq!(methods_and_uris[5].0, Method::GET);
    assert!(
        methods_and_uris[5].1.contains("/statefulsets/demo-broker"),
        "sixth request should be the statefulset status read: {}",
        methods_and_uris[5].1
    );

    assert_eq!(methods_and_uris[6].0, Method::PATCH);
    assert!(
        methods_and_uris[6].1.contains("/kafkas/demo/status"),
        "seventh request should be the kafka status patch: {}",
        methods_and_uris[6].1
    );

    assert_eq!(
        state.remaining_rules(),
        0,
        "every preloaded rule should have been consumed"
    );
}

#[tokio::test]
async fn reconcile_status_ready_true_when_sts_ready() {
    let (ctx, state) = build_ctx("demo", "y", happy_path_rules("demo", "y", Some(1)));
    let kafka = kafka_cr("demo", "y", 1);

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH must have been captured");

    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["type"], "Ready", "body = {body}");
    assert_eq!(cond["status"], "True", "body = {body}");
    assert_eq!(cond["reason"], "Available", "body = {body}");
    assert_eq!(body["status"]["replicas"], 1, "body = {body}");
    assert_eq!(body["status"]["readyReplicas"], 1, "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}

#[tokio::test]
async fn reconcile_status_ready_false_when_sts_partial() {
    // ready_replicas=Some(0) means the STS exists with replicas=1 but
    // nothing ready yet — reason should be NoBrokersReady.
    let (ctx, state) = build_ctx("demo", "y", happy_path_rules("demo", "y", Some(0)));
    let kafka = kafka_cr("demo", "y", 1);

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH must have been captured");

    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["type"], "Ready", "body = {body}");
    assert_eq!(cond["status"], "False", "body = {body}");
    assert_eq!(cond["reason"], "NoBrokersReady", "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}

#[tokio::test]
async fn reconcile_validation_rejects_replicas_two() {
    // For the validation path, only the status PATCH should fire; the
    // service/configmap/secret/statefulset apply branch is skipped entirely.
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/kafkas/demo/status".into(),
        response: json_response(200, &fake_kafka_body("demo", "y")),
    }];
    let (ctx, state) = build_ctx("demo", "y", rules);
    let kafka = kafka_cr("demo", "y", 2);

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // Exactly one captured request — the status PATCH — and no other
    // resource verbs leaked through.
    assert_eq!(
        observed.len(),
        1,
        "validation path should issue exactly one request, saw: {:?}",
        observed
            .iter()
            .map(|r| (r.method().clone(), r.uri().to_string()))
            .collect::<Vec<_>>()
    );

    for req in &observed {
        let uri = req.uri().to_string();
        assert!(
            !uri.contains("/services/"),
            "validation path must not touch services: {uri}"
        );
        assert!(
            !uri.contains("/configmaps/"),
            "validation path must not touch configmaps: {uri}"
        );
        assert!(
            !uri.contains("/secrets"),
            "validation path must not touch secrets: {uri}"
        );
        assert!(
            !uri.contains("/statefulsets/"),
            "validation path must not touch statefulsets: {uri}"
        );
    }

    let status_patch = &observed[0];
    assert_eq!(status_patch.method(), Method::PATCH);
    assert!(
        status_patch
            .uri()
            .to_string()
            .contains("/kafkas/demo/status"),
        "uri = {}",
        status_patch.uri()
    );
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["type"], "Ready");
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "UnsupportedReplicaCount");

    assert_eq!(state.remaining_rules(), 0);
}
