//! Mocked-client integration tests for the slice-20 `Kafka` reconciler.
//!
//! Slice 20: `Kafka` is the parent/coordinator. It owns the cluster-level
//! `Service`, `ConfigMap`, and cluster-id `Secret`, lists sibling
//! `KafkaNodePool`s by label, and aggregates their statuses. Broker
//! `StatefulSet`s live on the pool reconciler — the Kafka reconciler
//! must never touch `/statefulsets/`.
//!
//! Request sequence on a fresh Kafka:
//!   1. PATCH services/<name>-broker-headless   (SSA)
//!   2. PATCH configmaps/<name>-broker-config   (SSA)
//!   3. GET   secrets/<name>-cluster-id         (-> 404)
//!   4. POST  secrets                           (-> 201)
//!   5. GET   kafkanodepools?labelSelector=...  (-> 200 `KafkaNodePoolList`)
//!   6. PATCH kafkas/<name>/status              (merge)

use std::sync::Arc;

use crabka_operator::controller::kafka::reconcile;
use crabka_operator::crd::{Kafka, KafkaSpec};
use http::{Method, Response};
use serde_json::json;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState, fake_configmap_body, fake_kafka_body, fake_pool_body, fake_pool_list_body,
    fake_pool_list_item, fake_secret_body, fake_service_body, fixture_ctx, json_response,
    mock_client, not_found_body,
};

fn kafka_cr(name: &str, namespace: &str) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Build the rule list for a happy-path reconcile of `<name>` in
/// `<namespace>`. The caller controls the rendered pool list (and thus
/// the rolled-up status reason) via `pool_items`.
fn happy_path_rules(
    name: &str,
    namespace: &str,
    pool_items: &[serde_json::Value],
) -> Vec<MockRule> {
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");

    let mut rules = vec![
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
        // 5. GET kafkanodepools (list by label).
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{namespace}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(pool_items)),
        },
    ];
    // 5b. PATCH each pool to inject the controller owner-ref. The pool
    //     reconciler doesn't set this itself — the Kafka reconciler is
    //     the one that adopts existing pools labeled
    //     `crabka.io/cluster=<this>`. Without these owner-refs, deleting
    //     the Kafka CR doesn't cascade to the pool's StatefulSet, which
    //     the operator-e2e GC step asserts on.
    for item in pool_items {
        let pool_name = item["metadata"]["name"]
            .as_str()
            .expect("fake pool item has metadata.name");
        rules.push(MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}?"),
            response: json_response(200, &fake_pool_body(pool_name, namespace, name)),
        });
    }
    // 6. PATCH kafkas/<name>/status
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkas/{name}/status"),
        response: json_response(200, &fake_kafka_body(name, namespace)),
    });
    rules
}

fn build_ctx(
    namespace: &str,
    rules: Vec<MockRule>,
) -> (Arc<crabka_operator::context::Context>, Arc<MockState>) {
    let state = MockState::new(rules);
    let client = mock_client(&state, namespace);
    (Arc::new(fixture_ctx(client, namespace)), state)
}

#[tokio::test]
async fn kafka_applies_service_configmap_secret_no_statefulset() {
    // One pool present so we exercise the full sequence (otherwise the
    // status branch is identical, but a present pool makes the
    // "no StatefulSet" assertion meaningful).
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr("demo", "y");

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let methods_and_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    assert_eq!(
        observed.len(),
        7,
        "expected exactly 7 requests (svc, cm, get-secret, post-secret, list-pools, \
         patch-pool-owner-ref, status), saw {}: {:?}",
        observed.len(),
        methods_and_uris,
    );

    // No request must touch /statefulsets/ — that's the pool reconciler.
    for (method, uri) in &methods_and_uris {
        assert!(
            !uri.contains("/statefulsets/"),
            "Kafka reconciler must not touch statefulsets: {method} {uri}",
        );
    }

    assert_eq!(methods_and_uris[0].0, Method::PATCH);
    assert!(
        methods_and_uris[0]
            .1
            .contains("/services/demo-broker-headless"),
        "step 1 should patch the service: {}",
        methods_and_uris[0].1
    );

    assert_eq!(methods_and_uris[1].0, Method::PATCH);
    assert!(
        methods_and_uris[1]
            .1
            .contains("/configmaps/demo-broker-config"),
        "step 2 should patch the configmap: {}",
        methods_and_uris[1].1
    );

    assert_eq!(methods_and_uris[2].0, Method::GET);
    assert!(
        methods_and_uris[2].1.contains("/secrets/demo-cluster-id"),
        "step 3 should get the cluster-id secret: {}",
        methods_and_uris[2].1
    );

    assert_eq!(methods_and_uris[3].0, Method::POST);
    assert!(
        methods_and_uris[3].1.contains("/namespaces/y/secrets"),
        "step 4 should create the cluster-id secret: {}",
        methods_and_uris[3].1
    );

    assert_eq!(methods_and_uris[4].0, Method::GET);
    assert!(
        methods_and_uris[4].1.contains("/kafkanodepools"),
        "step 5 should list kafkanodepools: {}",
        methods_and_uris[4].1
    );
    assert!(
        methods_and_uris[4].1.contains("labelSelector="),
        "step 5 should filter by labelSelector: {}",
        methods_and_uris[4].1
    );

    assert_eq!(methods_and_uris[5].0, Method::PATCH);
    assert!(
        methods_and_uris[5].1.contains("/kafkanodepools/brokers"),
        "step 6 should patch the pool's owner-refs: {}",
        methods_and_uris[5].1
    );

    assert_eq!(methods_and_uris[6].0, Method::PATCH);
    assert!(
        methods_and_uris[6].1.contains("/kafkas/demo/status"),
        "step 7 should patch Kafka status: {}",
        methods_and_uris[6].1
    );

    assert_eq!(
        state.remaining_rules(),
        0,
        "every preloaded rule should have been consumed"
    );
}

#[tokio::test]
async fn kafka_status_no_node_pools_when_list_empty() {
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &[]));
    let kafka = kafka_cr("demo", "y");

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
    assert_eq!(cond["reason"], "NoNodePools", "body = {body}");
    assert_eq!(body["status"]["replicas"], json!(0), "body = {body}");
    assert_eq!(body["status"]["readyReplicas"], json!(0), "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}

#[tokio::test]
async fn kafka_status_aggregates_pool_readyreplicas() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr("demo", "y");

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
    assert_eq!(body["status"]["replicas"], json!(1), "body = {body}");
    assert_eq!(body["status"]["readyReplicas"], json!(1), "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}
