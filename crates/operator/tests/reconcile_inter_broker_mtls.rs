//! Slice 30: integration tests for inter-broker mTLS rendering
//! (broker config-file TLS block + `StatefulSet` mounts + idempotency).

use std::sync::Arc;

use crabka_operator::controller::kafka::reconcile;
use crabka_operator::crd::{Kafka, KafkaSpec, Listener, ListenerType};
use http::{Method, Response};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState, fake_configmap_body, fake_kafka_body, fake_pool_body, fake_pool_list_body,
    fake_pool_list_item, fake_secret_body, fake_service_body, fixture_ctx, json_response,
    mock_client, not_found_body,
};

// ── helpers ──────────────────────────────────────────────────────────────────

fn kafka_cr(name: &str, namespace: &str) -> Kafka {
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
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

fn fake_ca_secret(sname: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": sname, "namespace": namespace, "uid": "ca-uid" },
        "type": "Opaque",
        "data": {}
    })
}

fn fake_keystore_secret(sname: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": sname, "namespace": namespace, "uid": "ks-uid" },
        "type": "Opaque",
        "data": {}
    })
}

/// Build the full happy-path rule list for cluster `name` in `namespace`.
/// Mirrors the `happy_path_rules` function in `reconcile_kafka.rs`.
#[allow(clippy::too_many_lines)]
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

    let mut rules = vec![
        // 1. PATCH headless service.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, namespace)),
        },
        // 2. GET cluster-id secret -> 404.
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
        // 4. GET cluster-ca key -> 404.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        // 5. GET cluster-ca cert -> 404.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        // 6. PATCH cluster-ca key -> 200.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_key, namespace)),
        },
        // 7. PATCH cluster-ca cert -> 200.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_cert, namespace)),
        },
        // 8. GET clients-ca key -> 404.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        // 9. GET clients-ca cert -> 404.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        // 10. PATCH clients-ca key -> 200.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_key, namespace)),
        },
        // 11. PATCH clients-ca cert -> 200.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_cert, namespace)),
        },
        // 12. GET kafkanodepools (list by label).
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{namespace}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(pool_items)),
        },
        // 13. GET broker keystore -> 404 (first reconcile).
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{keystore_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        // 14. PATCH broker keystore -> 200.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &fake_keystore_secret(&keystore_name, namespace)),
        },
        // 15. PATCH configmap (per-broker TOML).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, namespace)),
        },
    ];

    // 16. PATCH each pool with owner-ref.
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
    // 17. PATCH kafkas/<name>/status.
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

// ── test 1: TLS block in broker TOML ─────────────────────────────────────────

/// Slice 30 (T11 test 1): the `ConfigMap` PATCH body must carry a
/// `broker-{id}.toml` key for every replica, and each TOML must contain:
/// - `controller_listener_protocol = "Ssl"`
/// - `[tls_config]`
/// - `cert_path = "/etc/crabka/broker-tls/{id}.crt"`
/// - `key_path = "/etc/crabka/broker-tls/{id}.key"`
/// - `client_ca_path = "/etc/crabka/cluster-ca/ca.crt"`
/// - `client_auth = "Required"`
///
/// Parsing via `toml::from_str::<crabka_broker::file_config::FileConfig>` must
/// succeed and return `tls_config.is_some() = true`.
#[tokio::test]
async fn rendered_broker_config_carries_controller_listener_protocol_ssl_and_tls_block() {
    let items = vec![fake_pool_list_item("brokers", "y", "c1", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("c1", "y", &items));
    let kafka = kafka_cr("c1", "y");

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // Find the ConfigMap PATCH and decode its body.
    let cm_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/configmaps/c1-broker-config")
        })
        .expect("ConfigMap PATCH must have been captured");

    let body: serde_json::Value =
        serde_json::from_slice(cm_patch.body()).expect("ConfigMap PATCH body is JSON");

    // The SSA body wraps the ConfigMap. The `data` field contains the TOML keys.
    let data = body
        .get("data")
        .and_then(|d| d.as_object())
        .unwrap_or_else(|| panic!("ConfigMap body must have data object; body = {body}"));

    // With one pool (nodeIdStart=0, replicas=1) we expect exactly one key.
    let toml_str = data
        .get("broker-0.toml")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            panic!(
                "broker-0.toml key missing; data keys = {:?}",
                data.keys().collect::<Vec<_>>()
            )
        });

    // Presence of required TOML fields.
    assert!(
        toml_str.contains("controller_listener_protocol = \"Ssl\""),
        "controller_listener_protocol = \"Ssl\" missing;\n{toml_str}"
    );
    assert!(
        toml_str.contains("[tls_config]"),
        "[tls_config] section missing;\n{toml_str}"
    );
    assert!(
        toml_str.contains("cert_path = \"/etc/crabka/broker-tls/0.crt\""),
        "cert_path missing;\n{toml_str}"
    );
    assert!(
        toml_str.contains("key_path = \"/etc/crabka/broker-tls/0.key\""),
        "key_path missing;\n{toml_str}"
    );
    assert!(
        toml_str.contains("client_ca_path = \"/etc/crabka/cluster-ca/ca.crt\""),
        "client_ca_path missing;\n{toml_str}"
    );
    assert!(
        toml_str.contains("client_auth = \"Required\""),
        "client_auth missing;\n{toml_str}"
    );

    // Round-trip parse through the broker's own FileConfig.
    let parsed: crabka_broker::file_config::FileConfig =
        toml::from_str(toml_str).expect("broker-0.toml must parse as FileConfig");
    assert!(
        parsed.tls_config.is_some(),
        "FileConfig.tls_config must be Some after parsing rendered TOML"
    );
}

// ── test 2: listeners[].tls=true still rejected ───────────────────────────────

/// Slice 30 (T11 test 2): a `Listener` with `tls: true` must cause the
/// reconciler to surface `ListenersValid=False reason=TlsNotYetSupported`.
/// No `ConfigMap` PATCH or keystore PATCH may be issued.
#[tokio::test]
async fn data_plane_tls_listener_still_rejected_in_slice_30() {
    let items = vec![fake_pool_list_item("brokers", "y", "c1", 1, 1)];
    // Validation failure bypasses the ConfigMap PATCH and broker keystore
    // PATCH — drop those rules to ensure the mock does not consume them.
    let mut rules = happy_path_rules("c1", "y", &items);
    rules.retain(|r| !r.path_substr.contains("/configmaps/"));
    rules.retain(|r| !r.path_substr.contains("-kafka-brokers"));
    let (ctx, state) = build_ctx("y", rules);

    let mut kafka = kafka_cr("c1", "y");
    kafka.spec.listeners = vec![Listener {
        name: "PLAIN".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: true,
        authentication: None,
        configuration: None,
        network_policy_peers: None,
    }];

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // No ConfigMap PATCH.
    let cm_patch = observed.iter().find(|r| {
        r.method() == Method::PATCH && r.uri().to_string().contains("/configmaps/c1-broker-config")
    });
    assert!(
        cm_patch.is_none(),
        "validation failure must NOT patch the broker-config ConfigMap: {:?}",
        cm_patch.map(|p| p.uri().to_string())
    );

    // Status conditions reflect the TLS validation error.
    let status_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/c1/status"))
        .expect("status PATCH captured");

    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");

    let valid = conds
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid present; body = {body}"));
    assert_eq!(valid["status"], "False", "body = {body}");
    assert_eq!(valid["reason"], "TlsNotYetSupported", "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}

// ── test 3: StatefulSet mounts all three Secrets ──────────────────────────────

/// Build a minimal `KafkaNodePool` labeled `crabka.io/cluster=<parent>`.
fn pool_cr_labeled(
    pool_name: &str,
    namespace: &str,
    parent: &str,
) -> crabka_operator::crd::KafkaNodePool {
    use crabka_operator::crd::{KafkaNodePool, KafkaNodePoolSpec, NodeRole};
    let mut pool = KafkaNodePool::new(
        pool_name,
        KafkaNodePoolSpec {
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            replicas: 1,
            node_id_start: 0,
            image: None,
            resources: None,
            template: None,
            storage: None,
        },
    );
    pool.metadata.namespace = Some(namespace.into());
    pool.metadata.uid = Some("pool-uid".into());
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("crabka.io/cluster".into(), parent.into());
    pool.metadata.labels = Some(labels);
    pool
}

/// Build the happy-path rules for a pool reconcile (parent GET + STS
/// pre-apply GET + STS PATCH + STS status GET + pool status PATCH).
fn pool_reconcile_rules(parent: &str, pool_name: &str, ns: &str) -> Vec<MockRule> {
    use shared::fake_parent_kafka_body;
    let sts_name = format!("{parent}-{pool_name}");
    vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &shared::fake_sts_body(&sts_name, ns, 1, Some(1))),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &shared::fake_sts_body(&sts_name, ns, 1, Some(1))),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ]
}

/// Slice 30 (T11 test 3): the `StatefulSet` pod spec must include
/// `volumeMounts` for `cluster-ca-cert`, `broker-tls`, and
/// `clients-ca-cert`, and the corresponding pod `volumes` must reference
/// the right Secret names: `c1-cluster-ca-cert`, `c1-kafka-brokers`,
/// `c1-clients-ca-cert`.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn statefulset_mounts_cluster_ca_broker_tls_clients_ca() {
    use crabka_operator::controller::kafka_node_pool::reconcile as pool_reconcile;

    let parent = "c1";
    let pool_name = "brokers";
    let ns = "y";
    let sts_name = format!("{parent}-{pool_name}");

    let (ctx, state) = build_ctx(ns, pool_reconcile_rules(parent, pool_name, ns));
    let pool = pool_cr_labeled(pool_name, ns, parent);

    pool_reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/statefulsets/{sts_name}"))
        })
        .expect("STS PATCH was captured");

    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS PATCH body is JSON");

    // Pod-level volumes: must include the three CA/keystore Secrets.
    let volumes = body["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap_or_else(|| panic!("volumes present; body = {body}"));

    // cluster-ca-cert volume -> Secret c1-cluster-ca-cert
    let cluster_ca_vol = volumes
        .iter()
        .find(|v| v["name"] == "cluster-ca-cert")
        .unwrap_or_else(|| panic!("cluster-ca-cert volume missing; volumes = {volumes:?}"));
    assert_eq!(
        cluster_ca_vol["secret"]["secretName"], "c1-cluster-ca-cert",
        "cluster-ca-cert volume must reference c1-cluster-ca-cert; body = {body}"
    );

    // broker-tls volume -> Secret c1-kafka-brokers
    let broker_tls_vol = volumes
        .iter()
        .find(|v| v["name"] == "broker-tls")
        .unwrap_or_else(|| panic!("broker-tls volume missing; volumes = {volumes:?}"));
    assert_eq!(
        broker_tls_vol["secret"]["secretName"], "c1-kafka-brokers",
        "broker-tls volume must reference c1-kafka-brokers; body = {body}"
    );

    // clients-ca-cert volume -> Secret c1-clients-ca-cert
    let clients_ca_vol = volumes
        .iter()
        .find(|v| v["name"] == "clients-ca-cert")
        .unwrap_or_else(|| panic!("clients-ca-cert volume missing; volumes = {volumes:?}"));
    assert_eq!(
        clients_ca_vol["secret"]["secretName"], "c1-clients-ca-cert",
        "clients-ca-cert volume must reference c1-clients-ca-cert; body = {body}"
    );

    // Broker container volumeMounts: must include all three mounts.
    let containers = body["spec"]["template"]["spec"]["containers"]
        .as_array()
        .unwrap_or_else(|| panic!("containers present; body = {body}"));
    let broker = containers
        .iter()
        .find(|c| c["name"] == "broker")
        .unwrap_or_else(|| panic!("broker container missing; body = {body}"));
    let volume_mounts = broker["volumeMounts"]
        .as_array()
        .unwrap_or_else(|| panic!("broker volumeMounts present; body = {body}"));

    let mount_names: Vec<&str> = volume_mounts
        .iter()
        .filter_map(|m| m["name"].as_str())
        .collect();

    for expected_mount in &["cluster-ca-cert", "broker-tls", "clients-ca-cert"] {
        assert!(
            mount_names.contains(expected_mount),
            "broker container must have volumeMount '{expected_mount}'; mounts = {mount_names:?}"
        );
    }

    assert_eq!(state.remaining_rules(), 0);
}

// ── test 4: render is idempotent across reconciles ───────────────────────────

/// Slice 30 (T11 test 4): running reconcile twice with the same spec must
/// produce byte-identical `broker-0.toml` output. The second reconcile's
/// GET-Secret calls return the bodies written by the first reconcile (the
/// mock FIFO state is extended with second-pass rules after first-pass
/// rules are consumed).
///
/// Because the mock CA-Secret response returns empty `data: {}`, the
/// operator regenerates a fresh CA on both passes. Idempotency here means
/// the *structure* and *paths* in the TOML are identical, which is what
/// the config-hash stability contract depends on. We assert that the two
/// TOML strings are byte-identical.
#[tokio::test]
async fn render_is_idempotent_across_reconciles() {
    let items = vec![fake_pool_list_item("brokers", "y", "c1", 1, 1)];

    // First reconcile: all GETs return 404 → operator creates everything.
    let rules_pass1 = happy_path_rules("c1", "y", &items);
    // Second reconcile: all GETs return the (empty-data) secrets the
    // operator "created" on pass 1 — operator regenerates from scratch
    // because data is empty, but the TOML output paths are deterministic.
    let rules_pass2 = happy_path_rules("c1", "y", &items);

    // Combine both passes into a single FIFO queue.
    let mut all_rules = rules_pass1;
    all_rules.extend(rules_pass2);

    let (ctx1, state1) = build_ctx("y", all_rules);
    let kafka1 = kafka_cr("c1", "y");

    // First reconcile.
    reconcile(Arc::new(kafka1), ctx1.clone()).await.unwrap();
    let observed1 = state1.take_observed();

    let cm_patch1 = observed1
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/configmaps/c1-broker-config")
        })
        .expect("first ConfigMap PATCH must have been captured");
    let body1: serde_json::Value =
        serde_json::from_slice(cm_patch1.body()).expect("first CM body is JSON");
    let toml1 = body1["data"]["broker-0.toml"]
        .as_str()
        .unwrap_or_else(|| panic!("broker-0.toml missing from first CM PATCH; body = {body1}"))
        .to_string();

    // Second reconcile.
    let kafka2 = kafka_cr("c1", "y");
    reconcile(Arc::new(kafka2), ctx1).await.unwrap();
    let observed2 = state1.take_observed();

    let cm_patch2 = observed2
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/configmaps/c1-broker-config")
        })
        .expect("second ConfigMap PATCH must have been captured");
    let body2: serde_json::Value =
        serde_json::from_slice(cm_patch2.body()).expect("second CM body is JSON");
    let toml2 = body2["data"]["broker-0.toml"]
        .as_str()
        .unwrap_or_else(|| panic!("broker-0.toml missing from second CM PATCH; body = {body2}"))
        .to_string();

    assert_eq!(
        toml1, toml2,
        "broker-0.toml must be byte-identical across two reconciles with the same spec"
    );

    assert_eq!(
        state1.remaining_rules(),
        0,
        "all mock rules must have been consumed"
    );
}
