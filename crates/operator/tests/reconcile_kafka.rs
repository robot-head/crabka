//! Mocked-client integration tests for the slice-20 `Kafka` reconciler.
//!
//! Slice 20: `Kafka` is the parent/coordinator. It owns the cluster-level
//! `Service`, `ConfigMap`, and cluster-id `Secret`, lists sibling
//! `KafkaNodePool`s by label, and aggregates their statuses. Broker
//! `StatefulSet`s live on the pool reconciler — the Kafka reconciler
//! must never touch `/statefulsets/`.
//!
//! Request sequence on a fresh Kafka with no `spec.listeners` set
//! (slice 25 synthesized internal-default path):
//!   1. PATCH services/<name>-broker-headless   (SSA)
//!   2. GET   secrets/<name>-cluster-id         (-> 404)
//!   3. POST  secrets                           (-> 201)
//!   4. GET   kafkanodepools?labelSelector=...  (-> 200 `KafkaNodePoolList`)
//!   5. PATCH configmaps/<name>-broker-config   (SSA, populated with per-broker TOML)
//!   6. PATCH kafkanodepools/<pool>             (owner-ref adopt)
//!   7. PATCH kafkas/<name>/status              (merge)
//!
//! The `ConfigMap` moved after the pool list because slice 25 derives one
//! `broker-{id}.toml` key per pool — we have to enumerate the pools
//! first to know which keys to emit.

use std::sync::Arc;

use crabka_operator::controller::kafka::reconcile;
use crabka_operator::crd::{Kafka, KafkaSpec, Listener, ListenerType};
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
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Variant carrying a `spec.config` for slice-21 tests. Uses
/// `log.retention.hours=24` because the plan pins the expected hash on
/// exactly that key/value pair.
fn kafka_cr_with_config(
    name: &str,
    namespace: &str,
    config: std::collections::BTreeMap<String, String>,
) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            config: Some(config),
            listeners: vec![],
            inter_broker_listener_name: None,
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
        // 1. PATCH headless service.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, namespace)),
        },
        // 2. GET cluster-id secret -> 404 (slice-20 one-shot create).
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{secret_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("secret not found"))
                .expect("404 builds"),
        },
        // 3. POST secret -> 201.
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
        // 4. GET kafkanodepools (list by label).
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{namespace}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(pool_items)),
        },
        // 5. PATCH configmap (per-broker TOML keys derived from the pool list).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, namespace)),
        },
    ];
    // 6. PATCH each pool to inject the controller owner-ref. The pool
    //    reconciler doesn't set this itself — the Kafka reconciler is
    //    the one that adopts existing pools labeled
    //    `crabka.io/cluster=<this>`. Without these owner-refs, deleting
    //    the Kafka CR doesn't cascade to the pool's StatefulSet, which
    //    the operator-e2e GC step asserts on.
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
    // 7. PATCH kafkas/<name>/status
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
        "expected exactly 7 requests (svc, get-secret, post-secret, list-pools, cm, \
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

    assert_eq!(methods_and_uris[1].0, Method::GET);
    assert!(
        methods_and_uris[1].1.contains("/secrets/demo-cluster-id"),
        "step 2 should get the cluster-id secret: {}",
        methods_and_uris[1].1
    );

    assert_eq!(methods_and_uris[2].0, Method::POST);
    assert!(
        methods_and_uris[2].1.contains("/namespaces/y/secrets"),
        "step 3 should create the cluster-id secret: {}",
        methods_and_uris[2].1
    );

    assert_eq!(methods_and_uris[3].0, Method::GET);
    assert!(
        methods_and_uris[3].1.contains("/kafkanodepools"),
        "step 4 should list kafkanodepools: {}",
        methods_and_uris[3].1
    );
    assert!(
        methods_and_uris[3].1.contains("labelSelector="),
        "step 4 should filter by labelSelector: {}",
        methods_and_uris[3].1
    );

    assert_eq!(methods_and_uris[4].0, Method::PATCH);
    assert!(
        methods_and_uris[4]
            .1
            .contains("/configmaps/demo-broker-config"),
        "step 5 should patch the configmap (after pool enumeration): {}",
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
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");
    let ready = conds
        .iter()
        .find(|c| c["type"] == "Ready")
        .expect("Ready condition present");
    assert_eq!(ready["status"], "True", "body = {body}");
    assert_eq!(ready["reason"], "Available", "body = {body}");
    assert_eq!(body["status"]["replicas"], json!(1), "body = {body}");
    assert_eq!(body["status"]["readyReplicas"], json!(1), "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}

#[tokio::test]
async fn kafka_patches_pool_label_with_config_hash() {
    let mut cfg = std::collections::BTreeMap::new();
    cfg.insert("log.retention.hours".to_string(), "24".to_string());

    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr_with_config("demo", "y", cfg);
    let expected_hash = crabka_operator::controller::common::combined_config_hash(&kafka.spec);

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let pool_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkanodepools/brokers")
        })
        .expect("pool adopt PATCH must have been captured");

    let body: serde_json::Value =
        serde_json::from_slice(pool_patch.body()).expect("pool PATCH body is JSON");
    let hash = body["metadata"]["labels"]["crabka.io/config-hash"]
        .as_str()
        .unwrap_or_else(|| {
            panic!("expected metadata.labels[crabka.io/config-hash] str, body = {body}")
        });
    assert_eq!(hash, expected_hash, "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}

#[tokio::test]
async fn kafka_status_includes_rolling_condition_stable() {
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
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");
    let rolling = conds
        .iter()
        .find(|c| c["type"] == "Rolling")
        .unwrap_or_else(|| panic!("Rolling condition present, body = {body}"));
    assert_eq!(rolling["status"], "False", "body = {body}");
    assert_eq!(rolling["reason"], "Stable", "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}

/// Slice 25: when `spec.listeners` is empty the operator synthesizes a
/// single internal `PLAIN` listener. The status PATCH must include
/// `ListenersValid=True`, `ListenersReady=True`, and a one-entry
/// `listeners[]` array describing the synthesized listener.
#[tokio::test]
async fn kafka_status_synthesized_default_listener_is_valid_and_ready() {
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
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");

    let valid = conds
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid condition present, body = {body}"));
    assert_eq!(valid["status"], "True", "body = {body}");
    assert_eq!(valid["reason"], "Valid", "body = {body}");

    let ready = conds
        .iter()
        .find(|c| c["type"] == "ListenersReady")
        .unwrap_or_else(|| panic!("ListenersReady condition present, body = {body}"));
    assert_eq!(ready["status"], "True", "body = {body}");
    assert_eq!(ready["reason"], "Ready", "body = {body}");

    let listeners = body["status"]["listeners"]
        .as_array()
        .unwrap_or_else(|| panic!("status.listeners array, body = {body}"));
    assert_eq!(listeners.len(), 1, "body = {body}");
    assert_eq!(listeners[0]["name"], "PLAIN", "body = {body}");
    assert_eq!(listeners[0]["type"], "internal", "body = {body}");
    assert_eq!(
        listeners[0]["bootstrapServers"], "demo-broker-headless.y.svc.cluster.local:9092",
        "body = {body}"
    );

    assert_eq!(state.remaining_rules(), 0);
}

/// Slice 25: a `spec.listeners` entry with `tls=true` is rejected at
/// validation. The status PATCH must show `ListenersValid=False
/// reason=TlsNotYetSupported` and `ListenersReady=False
/// reason=ListenersInvalid`, and the `ConfigMap` PATCH must carry no
/// `broker-*.toml` keys (no broker should boot with an invalid spec).
#[tokio::test]
async fn kafka_invalid_listener_tls_blocks_broker_configmap_and_sets_conditions() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    // Validation failure must NOT patch the broker-config ConfigMap, so
    // drop that rule from the happy-path set. Path-substr `/configmaps/`
    // is unique enough among the rule URIs to identify it.
    let mut rules = happy_path_rules("demo", "y", &items);
    rules.retain(|r| !r.path_substr.contains("/configmaps/"));
    let (ctx, state) = build_ctx("y", rules);
    let mut kafka = kafka_cr("demo", "y");
    kafka.spec.listeners = vec![Listener {
        name: "PLAIN".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: true,
        configuration: None,
    }];

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // Validation failure leaves the existing ConfigMap untouched —
    // stripping `broker-*.toml` keys would crash a previously-healthy
    // cluster on the next pod restart. Per the spec, "existing objects
    // are not deleted; surface the error and wait."
    let cm_patch = observed.iter().find(|r| {
        r.method() == Method::PATCH
            && r.uri()
                .to_string()
                .contains("/configmaps/demo-broker-config")
    });
    assert!(
        cm_patch.is_none(),
        "validation failure must NOT patch the broker-config ConfigMap: {:?}",
        cm_patch.map(|p| p.uri().to_string())
    );

    // Verify no per-broker / bootstrap external Services were rendered:
    for r in &observed {
        let uri = r.uri().to_string();
        assert!(
            !uri.contains("-bootstrap"),
            "no bootstrap Service should be applied for invalid listeners: {uri}"
        );
    }

    // Status conditions reflect the validation error.
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");

    let valid = conds
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid present, body = {body}"));
    assert_eq!(valid["status"], "False", "body = {body}");
    assert_eq!(valid["reason"], "TlsNotYetSupported", "body = {body}");

    let ready = conds
        .iter()
        .find(|c| c["type"] == "ListenersReady")
        .unwrap_or_else(|| panic!("ListenersReady present, body = {body}"));
    assert_eq!(ready["status"], "False", "body = {body}");
    assert_eq!(ready["reason"], "ListenersInvalid", "body = {body}");

    // status.listeners is empty on the validation-failure path.
    assert!(
        body["status"]["listeners"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "body = {body}"
    );

    assert_eq!(state.remaining_rules(), 0);
}
