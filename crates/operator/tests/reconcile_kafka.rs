//! Mocked-client integration tests for the `Kafka` reconciler.
//!
//! `Kafka` is the parent/coordinator. It owns the cluster-level
//! `Service`, `ConfigMap`, and cluster-id `Secret`, lists sibling
//! `KafkaNodePool`s by label, and aggregates their statuses. Broker
//! `StatefulSet`s live on the pool reconciler — the Kafka reconciler
//! must never touch `/statefulsets/`.
//!
//! Request sequence on a fresh Kafka with no `spec.listeners` set
//! (synthesized internal-default path):
//!   1. PATCH services/<name>-broker-headless   (SSA)
//!   2. GET   secrets/<name>-cluster-id         (-> 404)
//!   3. POST  secrets                           (-> 201)
//!   4. GET   kafkanodepools?labelSelector=...  (-> 200 `KafkaNodePoolList`)
//!   5. PATCH configmaps/<name>-broker-config   (SSA, populated with per-broker TOML)
//!   6. PATCH kafkanodepools/<pool>             (owner-ref adopt)
//!   7. PATCH kafkas/<name>/status              (merge)
//!
//! The `ConfigMap` comes after the pool list because the operator derives one
//! `broker-{id}.toml` key per pool — we have to enumerate the pools
//! first to know which keys to emit.

use std::sync::Arc;

use assert2::check;
use crabka_operator::{
    controller::kafka::reconcile,
    crd::{
        ConfigMapKeyRef, ExternalLoggingSource, Kafka, KafkaSpec, Listener, ListenerAuthentication,
        ListenerType, Logging, LoggingType, MetricsConfig, NetworkPolicySpec, PodMonitorSpec,
        ServiceMonitorSpec,
    },
};
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
            metadata_version: None,
            config: None,
            listeners: vec![],
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
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Variant carrying a `spec.metricsConfig`.
fn kafka_cr_with_metrics(name: &str, namespace: &str, metrics: Option<MetricsConfig>) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
            metrics_config: metrics,
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
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Variant carrying `spec.networkPolicy`.
fn kafka_cr_with_network_policy(
    name: &str,
    namespace: &str,
    network_policy: Option<NetworkPolicySpec>,
) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy,
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
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Variant carrying a `spec.config`. Uses
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
            metadata_version: None,
            config: Some(config),
            listeners: vec![],
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
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Build the rule list for a happy-path reconcile of `<name>` in
/// `<namespace>`. The caller controls the rendered pool list (and thus
/// the rolled-up status reason) via `pool_items`.
///
/// The rules cover CA and keystore secret lifecycle calls:
///   - 4 GET + 4 PATCH for cluster-ca + clients-ca secret pairs (no pre-existing CAs
///     → operator generates new ones).
///   - 1 GET + 1 PATCH for the broker keystore Secret (runs only in the
///     validation-ok branch, which `happy_path` always exercises).
#[allow(clippy::too_many_lines)] // the 10 CA+keystore rules make the function length inherent to mock-rule enumeration
fn happy_path_rules(
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

    let fake_ca_secret = |sname: &str| -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": sname, "namespace": namespace, "uid": "ca-uid" },
            "type": "Opaque",
            "data": {}
        })
    };
    let fake_keystore_secret = |sname: &str| -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": sname, "namespace": namespace, "uid": "ks-uid" },
            "type": "Opaque",
            "data": {}
        })
    };

    let mut rules = vec![
        // 1. PATCH headless service.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, namespace)),
        },
        // 2. GET cluster-id secret -> 404 (one-shot create).
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{secret_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("secret not found"))
                .expect("404 builds"),
        },
        // 3. POST cluster-id secret -> 201.
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
        // 4-7. Cluster CA — no pre-existing secrets → operator generates.
        //   GET cluster-ca key → 404
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        //   GET cluster-ca cert → 404
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        //   PATCH cluster-ca key → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_key)),
        },
        //   PATCH cluster-ca cert → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_cert)),
        },
        // 8-11. Clients CA — no pre-existing secrets → operator generates.
        //   GET clients-ca key → 404
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        //   GET clients-ca cert → 404
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        //   PATCH clients-ca key → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_key)),
        },
        //   PATCH clients-ca cert → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_cert)),
        },
        // 12. GET kafkanodepools (list by label).
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{namespace}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(pool_items)),
        },
        // 13-14. Broker keystore — no pre-existing → operator creates.
        //   GET keystore → 404
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{keystore_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        //   PATCH keystore → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &fake_keystore_secret(&keystore_name)),
        },
        // 15. PATCH configmap (per-broker TOML keys derived from the pool list).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, namespace)),
        },
    ];
    // 16. PATCH each pool to inject the controller owner-ref. The pool
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
    // 17. PATCH kafkas/<name>/status
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

    // The reconcile makes CA + keystore calls. With 1 pool the sequence is:
    //   1. PATCH service
    //   2. GET cluster-id secret (404)  3. POST cluster-id secret (201)
    //   4-7. GET/PATCH cluster-ca key+cert (new CA generated)
    //   8-11. GET/PATCH clients-ca key+cert (new CA generated)
    //   12. GET kafkanodepools
    //   13-14. GET/PATCH broker keystore
    //   15. PATCH configmap
    //   16. PATCH pool owner-ref
    //   17. PATCH kafka status
    assert2::assert!(observed.len() == 17);

    // No request must touch /statefulsets/ — that's the pool reconciler.
    for (_method, uri) in &methods_and_uris {
        assert2::assert!(!uri.contains("/statefulsets/"));
    }

    for (idx, want_method, want_substr, _what) in [
        (
            0,
            Method::PATCH,
            "/services/demo-broker-headless",
            "patch the service",
        ),
        (
            1,
            Method::GET,
            "/secrets/demo-cluster-id",
            "get the cluster-id secret",
        ),
        (
            2,
            Method::POST,
            "/namespaces/y/secrets",
            "create the cluster-id secret",
        ),
    ] {
        let (method, uri) = &methods_and_uris[idx];
        assert2::assert!(method == &want_method);
        assert2::assert!(uri.contains(want_substr));
    }

    // Steps 4-11: CA secret lifecycle.
    // After the POST, the next 8 requests are CA-related GETs and PATCHes.

    // Step 12: pool list.
    let pool_list_req = methods_and_uris
        .iter()
        .find(|(m, u)| *m == Method::GET && u.contains("/kafkanodepools"))
        .expect("GET kafkanodepools must be present");
    assert2::assert!(pool_list_req.1.contains("labelSelector="));

    // Configmap patch must occur after pool enumeration (it needs broker list).
    let cm_req = methods_and_uris
        .iter()
        .find(|(m, u)| *m == Method::PATCH && u.contains("/configmaps/demo-broker-config"))
        .expect("PATCH configmap must be present");
    let pool_list_idx = methods_and_uris
        .iter()
        .position(|(m, u)| *m == Method::GET && u.contains("/kafkanodepools"))
        .unwrap();
    let cm_idx = methods_and_uris
        .iter()
        .position(|(m, u)| *m == Method::PATCH && u.contains("/configmaps/demo-broker-config"))
        .unwrap();
    check!(
        cm_idx > pool_list_idx,
        "configmap patch ({cm_idx}) must come after pool list ({pool_list_idx}): {}",
        cm_req.1
    );

    // Pool owner-ref adopt.
    check!(
        methods_and_uris
            .iter()
            .any(|(m, u)| *m == Method::PATCH && u.contains("/kafkanodepools/brokers")),
        "pool owner-ref PATCH must be present",
    );

    // Status patch is last.
    assert2::assert!(&methods_and_uris[16].0 == &Method::PATCH);
    assert2::assert!(methods_and_uris[16].1.contains("/kafkas/demo/status"));

    check!(
        state.remaining_rules() == 0,
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
    assert2::assert!(cond["type"].as_str() == Some("Ready"));
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("NoNodePools"));
    assert2::assert!(body["status"]["replicas"].as_i64() == Some(0));
    assert2::assert!(body["status"]["readyReplicas"].as_i64() == Some(0));

    check!(state.remaining_rules() == 0);
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
    assert2::assert!(ready["status"].as_str() == Some("True"));
    assert2::assert!(ready["reason"].as_str() == Some("Available"));
    assert2::assert!(body["status"]["replicas"].as_i64() == Some(1));
    assert2::assert!(body["status"]["readyReplicas"].as_i64() == Some(1));

    check!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn kafka_patches_pool_label_with_config_hash() {
    let mut cfg = std::collections::BTreeMap::new();
    cfg.insert("log.retention.hours".to_string(), "24".to_string());

    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr_with_config("demo", "y", cfg);

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
    // The config-hash includes the generated CA cert PEM, so we
    // can't compute the expected hash upfront without access to the generated
    // material. Assert the hash is exactly 16 hex chars — that is the
    // contract config_hash produces (first 8 bytes of SHA-256, 2 hex chars
    // per byte), and is a tighter check than "non-empty".
    let hash = body["metadata"]["labels"]["crabka.io/config-hash"]
        .as_str()
        .unwrap_or_else(|| {
            panic!("expected metadata.labels[crabka.io/config-hash] str, body = {body}")
        });
    check!(
        hash.len() == 16,
        "config-hash must be exactly 16 hex chars, got {hash:?}, body = {body}"
    );
    check!(
        hash.chars().all(|c| c.is_ascii_hexdigit()),
        "config-hash must contain only hex digits, got {hash:?}, body = {body}",
    );

    check!(state.remaining_rules() == 0);
}

/// Variant carrying explicit `spec.kafkaVersion` + `spec.metadataVersion`
/// for version tests.
fn kafka_cr_with_versions(
    name: &str,
    namespace: &str,
    kafka_version: &str,
    metadata_version: Option<&str>,
) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: kafka_version.into(),
            metadata_version: metadata_version.map(str::to_string),
            config: None,
            listeners: vec![],
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
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Variant carrying `spec.logging`.
fn kafka_cr_with_logging(name: &str, namespace: &str, logging: Option<Logging>) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy: None,
            cluster_ca: None,
            clients_ca: None,
            logging,
            delegation_token: None,
            authorization: None,
            tiered_storage: None,
            inter_broker_kerberos: None,
            krb5_conf_secret_ref: None,
            tracing: None,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Pull the `LoggingReady` condition out of a status PATCH body.
fn logging_condition<B: AsRef<[u8]>>(observed: &[http::Request<B>]) -> serde_json::Value {
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body().as_ref()).expect("status PATCH body is JSON");
    body["status"]["conditions"]
        .as_array()
        .expect("conditions array")
        .iter()
        .find(|c| c["type"] == "LoggingReady")
        .cloned()
        .unwrap_or_else(|| panic!("LoggingReady present, body = {body}"))
}

/// Inline logging composes a sorted `RUST_LOG` filter into the
/// broker `ConfigMap`'s `rust.log` key and surfaces `LoggingReady=True`.
#[tokio::test]
async fn kafka_inline_logging_renders_rust_log_key() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let logging = Logging {
        r#type: LoggingType::Inline,
        loggers: [
            ("root".to_string(), "info".to_string()),
            ("crabka_broker".to_string(), "debug".to_string()),
        ]
        .into(),
        value_from: None,
    };
    let kafka = kafka_cr_with_logging("demo", "y", Some(logging));

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let data = configmap_data(&observed);
    assert2::assert!(data["rust.log"].as_str() == Some("crabka_broker=debug,info"));

    let cond = logging_condition(&observed);
    assert2::assert!(cond["status"].as_str() == Some("True"));
    assert2::assert!(cond["reason"].as_str() == Some("Available"));
    check!(state.remaining_rules() == 0);
}

/// A logging-unset cluster surfaces `LoggingReady=False/Disabled`
/// and renders no `rust.log` key.
#[tokio::test]
async fn kafka_no_logging_omits_rust_log_key() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr_with_logging("demo", "y", None);

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let data = configmap_data(&observed);
    assert2::assert!(data.get("rust.log").is_none());
    let cond = logging_condition(&observed);
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("Disabled"));
}

/// External logging reads the referenced `ConfigMap` key and uses it
/// verbatim as the `RUST_LOG` filter.
#[tokio::test]
async fn kafka_external_logging_reads_user_configmap() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    // The reconcile GETs the user-managed logging ConfigMap before rendering.
    rules.push(MockRule {
        method: Method::GET,
        path_substr: "/configmaps/my-log-cm".into(),
        response: json_response(
            200,
            &json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": "my-log-cm", "namespace": "y", "uid": "ext-cm" },
                "data": { "rust.log": "crabka_raft=trace,warn" }
            }),
        ),
    });
    let (ctx, state) = build_ctx("y", rules);
    let logging = Logging {
        r#type: LoggingType::External,
        loggers: std::collections::BTreeMap::default(),
        value_from: Some(ExternalLoggingSource {
            config_map_key_ref: ConfigMapKeyRef {
                name: "my-log-cm".into(),
                key: "rust.log".into(),
            },
        }),
    };
    let kafka = kafka_cr_with_logging("demo", "y", Some(logging));

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let data = configmap_data(&observed);
    assert2::assert!(data["rust.log"].as_str() == Some("crabka_raft=trace,warn"));
    let cond = logging_condition(&observed);
    assert2::assert!(cond["status"] == "True");
}

/// An external logging reference to a missing `ConfigMap` surfaces
/// `LoggingReady=False` and renders no `rust.log` key (the broker keeps its
/// built-in default filter).
#[tokio::test]
async fn kafka_external_logging_missing_configmap_surfaces_condition() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    rules.push(MockRule {
        method: Method::GET,
        path_substr: "/configmaps/absent-cm".into(),
        response: Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(not_found_body("configmap not found"))
            .expect("404 builds"),
    });
    let (ctx, state) = build_ctx("y", rules);
    let logging = Logging {
        r#type: LoggingType::External,
        loggers: std::collections::BTreeMap::default(),
        value_from: Some(ExternalLoggingSource {
            config_map_key_ref: ConfigMapKeyRef {
                name: "absent-cm".into(),
                key: "rust.log".into(),
            },
        }),
    };
    let kafka = kafka_cr_with_logging("demo", "y", Some(logging));

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let data = configmap_data(&observed);
    assert2::assert!(data.get("rust.log").is_none());
    let cond = logging_condition(&observed);
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("LoggingConfigMapNotFound"));
}

/// Find the broker-config `ConfigMap` PATCH and return its serialized data
/// map (`{ "broker-0.toml": "...", ... }`).
fn configmap_data<B: AsRef<[u8]>>(observed: &[http::Request<B>]) -> serde_json::Value {
    let cm_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains("/configmaps/demo-broker-config")
        })
        .expect("configmap PATCH must have been captured");
    let body: serde_json::Value =
        serde_json::from_slice(cm_patch.body().as_ref()).expect("configmap PATCH body is JSON");
    body["data"].clone()
}

/// A valid cluster echoes `kafkaVersion`, finalizes
/// `metadataVersion` in status, and surfaces `KafkaVersionValid=True`.
/// (`metadata.version` is finalized via the bootstrap feature record, not
/// rendered into broker config — see `render_configmap`.)
#[tokio::test]
async fn kafka_status_finalizes_metadata_version() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr_with_versions("demo", "y", "3.7.0", None);

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // Status echoes the versions and reports validity.
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    assert2::assert!(&body["status"]["kafkaVersion"] == &json!("3.7.0"));
    assert2::assert!(&body["status"]["metadataVersion"] == &json!("3.7"));
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");
    let vcond = conds
        .iter()
        .find(|c| c["type"] == "KafkaVersionValid")
        .unwrap_or_else(|| panic!("KafkaVersionValid present, body = {body}"));
    assert2::assert!(vcond["status"] == "True");

    assert2::assert!(state.remaining_rules() == 0);
}

/// A metadata version newer than the binary is rejected — no
/// roll, finalized version not advanced.
#[tokio::test]
async fn kafka_metadata_version_too_high_blocks() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr_with_versions("demo", "y", "3.6.0", Some("3.7"));

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
    let vcond = conds
        .iter()
        .find(|c| c["type"] == "KafkaVersionValid")
        .unwrap_or_else(|| panic!("KafkaVersionValid present, body = {body}"));
    assert2::assert!(vcond["status"].as_str() == Some("False"));
    assert2::assert!(vcond["reason"].as_str() == Some("MetadataVersionTooHigh"));
    // Finalized metadata version is not advanced (was never set).
    check!(
        body["status"]["metadataVersion"].is_null(),
        "metadataVersion must not be finalized on rejection, body = {body}"
    );

    check!(state.remaining_rules() == 0);
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
    assert2::assert!(rolling["status"].as_str() == Some("False"));
    assert2::assert!(rolling["reason"].as_str() == Some("Stable"));

    check!(state.remaining_rules() == 0);
}

/// When `spec.listeners` is empty the operator synthesizes a
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

    let ready = conds
        .iter()
        .find(|c| c["type"] == "ListenersReady")
        .unwrap_or_else(|| panic!("ListenersReady condition present, body = {body}"));
    assert2::assert!(valid["status"].as_str() == Some("True"));
    assert2::assert!(valid["reason"].as_str() == Some("Valid"));
    assert2::assert!(ready["status"].as_str() == Some("True"));
    assert2::assert!(ready["reason"].as_str() == Some("Ready"));

    let listeners = body["status"]["listeners"]
        .as_array()
        .unwrap_or_else(|| panic!("status.listeners array, body = {body}"));
    assert2::assert!(
        listeners.as_slice()
            == [json!({
                "name": "PLAIN",
                "type": "internal",
                "bootstrapServers": "demo-broker-headless.y.svc.cluster.local:9092",
                "addresses": [{
                    "host": "demo-brokers-0.demo-broker-headless.y.svc.cluster.local",
                    "port": 9092
                }]
            })]
    );

    check!(state.remaining_rules() == 0);
}

/// A listener with `authentication=Tls` (mTLS) but `tls=false`
/// (no transport TLS) is invalid. The status PATCH must show
/// `ListenersValid=False reason=ListenerMtlsRequiresTransportTls` and
/// `ListenersReady=False reason=ListenersInvalid`, and the `ConfigMap`
/// PATCH must be skipped (no broker should boot with an invalid spec).
#[tokio::test]
async fn kafka_mtls_without_tls_blocks_broker_configmap_and_sets_conditions() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    // Validation failure must NOT patch the broker-config ConfigMap or the
    // broker keystore Secret (both are inside the validation-ok branch), so
    // drop those rules from the happy-path set.
    let mut rules = happy_path_rules("demo", "y", &items);
    rules.retain(|r| !r.path_substr.contains("/configmaps/"));
    rules.retain(|r| !r.path_substr.contains("-kafka-brokers"));
    let (ctx, state) = build_ctx("y", rules);
    let mut kafka = kafka_cr("demo", "y");
    kafka.spec.listeners = vec![Listener {
        name: "PLAIN".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: false,
        authentication: Some(ListenerAuthentication::Tls),
        configuration: None,
        network_policy_peers: None,
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
    assert2::assert!(cm_patch.is_none());

    // Verify no per-broker / bootstrap external Services were rendered:
    for r in &observed {
        let uri = r.uri().to_string();
        assert2::assert!(!uri.contains("-bootstrap"));
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
    assert2::assert!(valid["status"].as_str() == Some("False"));
    assert2::assert!(valid["reason"].as_str() == Some("ListenerMtlsRequiresTransportTls"));

    let ready = conds
        .iter()
        .find(|c| c["type"] == "ListenersReady")
        .unwrap_or_else(|| panic!("ListenersReady present, body = {body}"));
    assert2::assert!(ready["status"].as_str() == Some("False"));
    assert2::assert!(ready["reason"].as_str() == Some("ListenersInvalid"));

    // status.listeners is empty on the validation-failure path.
    check!(
        body["status"]["listeners"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "body = {body}"
    );

    check!(state.remaining_rules() == 0);
}

/// Helper: pull the `MetricsReady` condition out of a status PATCH body.
fn metrics_ready_cond(body: &serde_json::Value) -> &serde_json::Value {
    body["status"]["conditions"]
        .as_array()
        .expect("conditions array")
        .iter()
        .find(|c| c["type"] == "MetricsReady")
        .unwrap_or_else(|| panic!("MetricsReady condition present, body = {body}"))
}

/// `metricsConfig` absent. No dynamic monitoring resources may
/// be applied, and the status carries `MetricsReady=False reason=Disabled`.
#[tokio::test]
async fn metrics_disabled_no_dynamic_apply() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr("demo", "y");

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    for r in &observed {
        let uri = r.uri().to_string();
        assert2::assert!(!uri.contains("/apis/monitoring.coreos.com/"));
        assert2::assert!(!uri.contains("/services/demo-broker-metrics"));
    }

    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let cond = metrics_ready_cond(&body);
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("Disabled"));

    check!(state.remaining_rules() == 0);
}

/// Faked apply-patch response that echoes a minimal `PodMonitor` body.
fn fake_pod_monitor_body(name: &str, namespace: &str) -> serde_json::Value {
    json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PodMonitor",
        "metadata": { "name": name, "namespace": namespace, "uid": "pm-uid" },
        "spec": { "selector": { "matchLabels": {} }, "podMetricsEndpoints": [] }
    })
}

/// Faked apply-patch response that echoes a minimal `ServiceMonitor` body.
fn fake_service_monitor_body(name: &str, namespace: &str) -> serde_json::Value {
    json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "ServiceMonitor",
        "metadata": { "name": name, "namespace": namespace, "uid": "sm-uid" },
        "spec": { "selector": { "matchLabels": {} }, "endpoints": [] }
    })
}

/// `podMonitor` set. Reconcile applies exactly one `PodMonitor`
/// via SSA against `monitoring.coreos.com/v1`, then best-effort deletes
/// the abandoned `ServiceMonitor` + metrics `Service`. The status surfaces
/// `MetricsReady=True reason=Available`.
#[tokio::test]
async fn pod_monitor_path_applies_one_resource() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    // Insert metrics rules before the trailing status PATCH so the FIFO
    // matcher consumes them in encounter order. The status PATCH rule is
    // the last entry produced by happy_path_rules.
    let status_rule = rules.pop().expect("status rule present");
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: "/apis/monitoring.coreos.com/v1/namespaces/y/podmonitors/demo-broker".into(),
        response: json_response(200, &fake_pod_monitor_body("demo-broker", "y")),
    });
    rules.push(MockRule {
        method: Method::DELETE,
        path_substr: "/apis/monitoring.coreos.com/v1/namespaces/y/servicemonitors/demo-broker"
            .into(),
        response: Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(not_found_body("servicemonitor not found"))
            .expect("404 builds"),
    });
    rules.push(MockRule {
        method: Method::DELETE,
        path_substr: "/api/v1/namespaces/y/services/demo-broker-metrics".into(),
        response: Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(not_found_body("service not found"))
            .expect("404 builds"),
    });
    rules.push(status_rule);

    let (ctx, state) = build_ctx("y", rules);
    let metrics = MetricsConfig {
        pod_monitor: Some(PodMonitorSpec::default()),
        ..Default::default()
    };
    let kafka = kafka_cr_with_metrics("demo", "y", Some(metrics));

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let pm_patches: Vec<&http::Request<hyper::body::Bytes>> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/podmonitors/demo-broker")
        })
        .collect();
    assert2::assert!(pm_patches.len() == 1);
    let uri = pm_patches[0].uri().to_string();
    check!(
        uri.contains("fieldManager=crabka-operator"),
        "PATCH must carry the operator's field manager: {uri}"
    );
    check!(
        uri.contains("force=true"),
        "PATCH must force-takeover: {uri}"
    );

    // No ServiceMonitor PATCH.
    check!(
        !observed.iter().any(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/servicemonitors/demo-broker")
        }),
        "pod_monitor path must not PATCH a ServiceMonitor"
    );

    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let cond = metrics_ready_cond(&body);
    assert2::assert!(cond["status"].as_str() == Some("True"));
    assert2::assert!(cond["reason"].as_str() == Some("Available"));

    check!(state.remaining_rules() == 0);
}

/// `serviceMonitor` set. Reconcile applies the headless metrics
/// `Service` and then the `ServiceMonitor`. The abandoned `PodMonitor` is
/// best-effort deleted. Status surfaces `MetricsReady=True reason=Available`.
#[tokio::test]
async fn service_monitor_path_applies_service_and_servicemonitor() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    let status_rule = rules.pop().expect("status rule present");
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: "/api/v1/namespaces/y/services/demo-broker-metrics".into(),
        response: json_response(200, &fake_service_body("demo-broker-metrics", "y")),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: "/apis/monitoring.coreos.com/v1/namespaces/y/servicemonitors/demo-broker"
            .into(),
        response: json_response(200, &fake_service_monitor_body("demo-broker", "y")),
    });
    rules.push(MockRule {
        method: Method::DELETE,
        path_substr: "/apis/monitoring.coreos.com/v1/namespaces/y/podmonitors/demo-broker".into(),
        response: Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(not_found_body("podmonitor not found"))
            .expect("404 builds"),
    });
    rules.push(status_rule);

    let (ctx, state) = build_ctx("y", rules);
    let metrics = MetricsConfig {
        service_monitor: Some(ServiceMonitorSpec::default()),
        ..Default::default()
    };
    let kafka = kafka_cr_with_metrics("demo", "y", Some(metrics));

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let svc_patches: Vec<&http::Request<hyper::body::Bytes>> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains("/services/demo-broker-metrics")
        })
        .collect();
    assert2::assert!(svc_patches.len() == 1);

    let sm_patches: Vec<&http::Request<hyper::body::Bytes>> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/servicemonitors/demo-broker")
        })
        .collect();
    assert2::assert!(sm_patches.len() == 1);

    // No PodMonitor PATCH.
    assert2::assert!(!observed.iter().any(|r| {
        r.method() == Method::PATCH && r.uri().to_string().contains("/podmonitors/demo-broker")
    }));

    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let cond = metrics_ready_cond(&body);
    assert2::assert!(cond["status"].as_str() == Some("True"));
    assert2::assert!(cond["reason"].as_str() == Some("Available"));

    check!(state.remaining_rules() == 0);
}

/// Both `podMonitor` and `serviceMonitor` set. Reconcile must
/// short-circuit before any dynamic apply and surface
/// `MetricsReady=False reason=MutuallyExclusive`. No request to the
/// monitoring API may be issued — the harness's fallback 404 would itself
/// fail the assertion below.
#[tokio::test]
async fn mutually_exclusive_sets_condition_and_skips_apply() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let metrics = MetricsConfig {
        pod_monitor: Some(PodMonitorSpec::default()),
        service_monitor: Some(ServiceMonitorSpec::default()),
        ..Default::default()
    };
    let kafka = kafka_cr_with_metrics("demo", "y", Some(metrics));

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    for r in &observed {
        let uri = r.uri().to_string();
        assert2::assert!(!uri.contains("/apis/monitoring.coreos.com/"));
        assert2::assert!(!uri.contains("/services/demo-broker-metrics"));
    }

    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let cond = metrics_ready_cond(&body);
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("MutuallyExclusive"));

    check!(state.remaining_rules() == 0);
}

/// The Prometheus Operator CRDs are not installed — the dynamic
/// PATCH against `monitoring.coreos.com/v1` 404s. Reconcile must surface
/// `MetricsReady=False reason=PrometheusOperatorCrdsMissing` rather than
/// fail; the status patch still lands.
#[tokio::test]
async fn prom_operator_missing_sets_condition() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    let status_rule = rules.pop().expect("status rule present");
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: "/apis/monitoring.coreos.com/v1/namespaces/y/podmonitors/demo-broker".into(),
        response: Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(not_found_body(
                "the server could not find the requested resource",
            ))
            .expect("404 builds"),
    });
    rules.push(status_rule);

    let (ctx, state) = build_ctx("y", rules);
    let metrics = MetricsConfig {
        pod_monitor: Some(PodMonitorSpec::default()),
        ..Default::default()
    };
    let kafka = kafka_cr_with_metrics("demo", "y", Some(metrics));

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let cond = metrics_ready_cond(&body);
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("PrometheusOperatorCrdsMissing"));

    check!(state.remaining_rules() == 0);
}

/// `spec.networkPolicy=None` (the default in `kafka_cr`)
/// must not touch `/networkpolicies/` at all and must surface
/// `NetworkPolicyReady=False reason=Disabled`.
#[tokio::test]
async fn network_policy_disabled_no_apply() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr("demo", "y");
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    for r in &observed {
        let uri = r.uri().to_string();
        assert2::assert!(!uri.contains("/networkpolicies/"));
    }

    // NetworkPolicyReady=False reason=Disabled present.
    let status = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    let cond = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "NetworkPolicyReady")
        .expect("NetworkPolicyReady condition present");
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("Disabled"));
}

/// `spec.networkPolicy=Some(NetworkPolicySpec::default())`
/// applies exactly one `NetworkPolicy` via SSA and surfaces
/// `NetworkPolicyReady=True reason=Available`.
#[tokio::test]
async fn network_policy_enabled_applies_one_resource() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    // Insert the NetworkPolicy apply rule before the trailing status PATCH.
    let mut rules = happy_path_rules("demo", "y", &items);
    let last_idx = rules.len() - 1;
    rules.insert(
        last_idx,
        MockRule {
            method: Method::PATCH,
            path_substr: "/networkpolicies/demo-broker-policy".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "metadata": {"name": "demo-broker-policy", "namespace": "y"},
                }),
            ),
        },
    );
    let (ctx, state) = build_ctx("y", rules);

    let kafka = kafka_cr_with_network_policy(
        "demo",
        "y",
        Some(crabka_operator::crd::NetworkPolicySpec::default()),
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let apply_count = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains("/networkpolicies/demo-broker-policy")
        })
        .count();
    assert2::assert!(apply_count == 1);

    let status = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/demo/status")
        })
        .expect("status PATCH");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    let cond = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "NetworkPolicyReady")
        .expect("NetworkPolicyReady present");
    assert2::assert!(cond["status"].as_str() == Some("True"));
    assert2::assert!(cond["reason"].as_str() == Some("Available"));
    check!(state.remaining_rules() == 0);
}

/// A Kafka CR with `status.conditions[NetworkPolicyReady].reason
/// = "Available"` and `spec.networkPolicy = None` issues exactly one
/// DELETE on `<name>-broker-policy` (orphan cleanup).
#[tokio::test]
async fn network_policy_transition_deletes_on_disable() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    let last_idx = rules.len() - 1;
    rules.insert(
        last_idx,
        MockRule {
            method: Method::DELETE,
            path_substr: "/networkpolicies/demo-broker-policy".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "kind": "Status", "apiVersion": "v1", "status": "Success",
                }),
            ),
        },
    );
    let (ctx, state) = build_ctx("y", rules);

    // Build a Kafka whose cached status already carries
    // NetworkPolicyReady=Available.
    let mut kafka = kafka_cr("demo", "y");
    kafka.status = Some(crabka_operator::crd::KafkaStatus {
        conditions: vec![crabka_operator::crd::KafkaCondition {
            type_: "NetworkPolicyReady".into(),
            status: "True".into(),
            reason: "Available".into(),
            message: "previously rendered".into(),
            last_transition_time: "2026-05-17T00:00:00Z".into(),
        }],
        replicas: Some(1),
        ready_replicas: Some(1),
        listeners: vec![],
        cluster_ca: None,
        clients_ca: None,
        kafka_version: None,
        metadata_version: None,
    });

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let deletes: Vec<_> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::DELETE
                && r.uri()
                    .to_string()
                    .contains("/networkpolicies/demo-broker-policy")
        })
        .collect();
    assert2::assert!(deletes.len() == 1);
}

/// Cold disable (no prior `NetworkPolicyReady=Available`) must
/// not call DELETE at all — avoids gratuitous API calls for clusters that
/// never opted into `NetworkPolicy`.
#[tokio::test]
async fn network_policy_cold_disable_no_delete() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "y", &items));
    let kafka = kafka_cr("demo", "y"); // no status, no networkPolicy
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let deletes_or_patches: Vec<_> = observed
        .iter()
        .filter(|r| r.uri().to_string().contains("/networkpolicies/"))
        .collect();
    assert2::assert!(deletes_or_patches.is_empty());
}

/// When one listener has `network_policy_peers=Some(vec![])`,
/// the rendered `NetworkPolicy` body sent on the PATCH must NOT contain a
/// per-listener rule with empty `from` for that listener's port. (The
/// operator-allow rule for that port is still present.)
#[tokio::test]
async fn network_policy_listener_deny_all_skips_port_rule() {
    let items = vec![fake_pool_list_item("brokers", "y", "demo", 1, 1)];
    let mut rules = happy_path_rules("demo", "y", &items);
    let last_idx = rules.len() - 1;
    rules.insert(
        last_idx,
        MockRule {
            method: Method::PATCH,
            path_substr: "/networkpolicies/demo-broker-policy".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "metadata": {"name": "demo-broker-policy", "namespace": "y"},
                }),
            ),
        },
    );
    let (ctx, state) = build_ctx("y", rules);

    let mut kafka = kafka_cr_with_network_policy("demo", "y", Some(NetworkPolicySpec::default()));
    kafka.spec.listeners = vec![Listener {
        name: "PLAIN".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: false,
        authentication: None,
        configuration: None,
        network_policy_peers: Some(vec![]),
    }];
    kafka.spec.inter_broker_listener_name = Some("PLAIN".into());

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let np_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains("/networkpolicies/demo-broker-policy")
        })
        .expect("NetworkPolicy PATCH captured");
    let body: serde_json::Value = serde_json::from_slice(np_patch.body()).unwrap();
    let ingress = body["spec"]["ingress"].as_array().expect("ingress array");

    // Count rules targeting :9092 with an empty `from` (would indicate
    // allow-all sneaking through for the deny-all listener).
    let allow_alls: Vec<_> = ingress
        .iter()
        .filter(|r| {
            let ports_match = r["ports"]
                .as_array()
                .is_some_and(|ps| ps.iter().any(|p| p["port"].as_i64() == Some(9092)));
            let from_empty = r["from"].as_array().is_some_and(Vec::is_empty);
            ports_match && from_empty
        })
        .collect();
    assert2::assert!(allow_alls.is_empty());
}
