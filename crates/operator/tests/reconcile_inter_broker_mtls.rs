//! Integration tests for inter-broker mTLS rendering
//! (broker config-file TLS block + `StatefulSet` mounts + idempotency).

use assert2::assert;
use std::sync::Arc;

use crabka_operator::controller::kafka::reconcile;
use crabka_operator::crd::{Kafka, KafkaSpec, Listener, ListenerType};
use http::{Method, Response};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, build_ctx, fake_pool_body, fake_pool_list_item, happy_path_rules, json_response,
    not_found_body,
};

// ── helpers ──────────────────────────────────────────────────────────────────

// fake_ca_secret, fake_keystore_secret, happy_path_rules, build_ctx are in shared/mod.rs.

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

// ── test 1: TLS block in broker TOML ─────────────────────────────────────────

/// The `ConfigMap` PATCH body must carry a
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
    for needle in [
        "controller_listener_protocol = \"Ssl\"",
        "[tls_config]",
        "cert_path = \"/etc/crabka/broker-tls/0.crt\"",
        "key_path = \"/etc/crabka/broker-tls/0.key\"",
        "client_ca_path = \"/etc/crabka/cluster-ca/ca.crt\"",
        "client_auth = \"Required\"",
    ] {
        assert!(toml_str.contains(needle), "{needle} missing;\n{toml_str}");
    }

    // Round-trip parse through the broker's own FileConfig.
    let parsed: crabka_broker::file_config::FileConfig =
        toml::from_str(toml_str).expect("broker-0.toml must parse as FileConfig");
    assert!(
        parsed.tls_config.is_some(),
        "FileConfig.tls_config must be Some after parsing rendered TOML"
    );
}

// ── test 2: tls=true, authentication=None listener reconciles (anonymous TLS) ──

/// A `Listener` with `tls: true` and no authentication
/// is now valid — it represents anonymous-over-TLS. Reconcile must succeed
/// and the rendered broker TOML must contain `protocol = "Ssl"` for that
/// listener.
#[tokio::test]
async fn data_plane_tls_listener_anonymous_now_reconciles() {
    let items = vec![fake_pool_list_item("brokers", "y", "c1", 1, 1)];
    // This is a valid listener now — use the full happy-path rules including
    // the ConfigMap PATCH and broker keystore PATCH.
    let (ctx, state) = build_ctx("y", happy_path_rules("c1", "y", &items));

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

    // ConfigMap PATCH must be present — reconcile succeeded.
    let cm_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/configmaps/c1-broker-config")
        })
        .expect("ConfigMap PATCH must be captured for a valid TLS listener");

    let body: serde_json::Value =
        serde_json::from_slice(cm_patch.body()).expect("ConfigMap PATCH body is JSON");
    let data = body
        .get("data")
        .and_then(|d| d.as_object())
        .unwrap_or_else(|| panic!("ConfigMap body must have data object; body = {body}"));

    let toml_str = data
        .get("broker-0.toml")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            panic!(
                "broker-0.toml key missing; data keys = {:?}",
                data.keys().collect::<Vec<_>>()
            )
        });

    // The anonymous-TLS listener must render as protocol = "Ssl".
    assert!(
        toml_str.contains("protocol = \"Ssl\""),
        "anonymous TLS listener must render protocol = \"Ssl\";\n{toml_str}"
    );

    // Status conditions must reflect success.
    let status_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/c1/status"))
        .expect("status PATCH captured");

    let sbody: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let conds = sbody["status"]["conditions"]
        .as_array()
        .expect("conditions array");

    let valid = conds
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid present; body = {sbody}"));
    assert!(valid["status"] == "True", "body = {sbody}");

    assert!(state.remaining_rules() == 0);
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

/// The `StatefulSet` pod spec must include
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
    assert!(
        cluster_ca_vol["secret"]["secretName"] == "c1-cluster-ca-cert",
        "cluster-ca-cert volume must reference c1-cluster-ca-cert; body = {body}"
    );

    // broker-tls volume -> Secret c1-kafka-brokers
    let broker_tls_vol = volumes
        .iter()
        .find(|v| v["name"] == "broker-tls")
        .unwrap_or_else(|| panic!("broker-tls volume missing; volumes = {volumes:?}"));
    assert!(
        broker_tls_vol["secret"]["secretName"] == "c1-kafka-brokers",
        "broker-tls volume must reference c1-kafka-brokers; body = {body}"
    );

    // clients-ca-cert volume -> Secret c1-clients-ca-cert
    let clients_ca_vol = volumes
        .iter()
        .find(|v| v["name"] == "clients-ca-cert")
        .unwrap_or_else(|| panic!("clients-ca-cert volume missing; volumes = {volumes:?}"));
    assert!(
        clients_ca_vol["secret"]["secretName"] == "c1-clients-ca-cert",
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

    assert!(state.remaining_rules() == 0);
}

// ── test 4: render is idempotent across reconciles ───────────────────────────

/// Running reconcile twice with the same spec must
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

    assert!(
        toml1 == toml2,
        "broker-0.toml must be byte-identical across two reconciles with the same spec"
    );

    assert!(
        state1.remaining_rules() == 0,
        "all mock rules must have been consumed"
    );
}
