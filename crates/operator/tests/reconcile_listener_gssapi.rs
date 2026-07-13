//! Integration tests for the `gssapi` listener authentication variant +
//! inter-broker Kerberos initiate config. Exercises the full reconcile
//! path against the kube-mock harness:
//!
//!   1. Happy path: a `type: gssapi` listener with its keytab Secret
//!      present renders the broker-global `[gssapi]` TOML block (with
//!      `keytab_path` + `enabled_mechanisms = ["GSSAPI"]`) into the
//!      broker-config `ConfigMap`, and the pool reconciler mounts the
//!      `gssapi-keytab` projected-items volume at
//!      `/etc/crabka/gssapi-keytab`.
//!   2. Missing keytab Secret: the cluster reconciler short-circuits to a
//!      `Ready=False` status condition with reason
//!      `MissingGssapiKeytabSecret` (same shape as the OAuth
//!      introspection missing-Secret path) and emits no `ConfigMap` PATCH.
//!   3. Inter-broker GSSAPI: when `interBrokerListenerName` resolves to
//!      the gssapi listener and `spec.interBrokerKerberos` is set, the
//!      `ConfigMap` TOML gains the `[inter_broker_credentials]` block with
//!      `type = "gssapi"` + the client principal.
//!   4. krb5.conf: `spec.krb5ConfSecretRef` drives a `krb5-conf` volume +
//!      `/etc/crabka/krb5` mount and a `KRB5_CONFIG` env on the broker
//!      container (asserted via the pool reconciler's `StatefulSet`).
//!
//! The pure validator + TOML render + pod-template render functions are
//! covered by unit tests inside `controller/{listeners,kafka_node_pool}.rs`.
//! The added value here is end-to-end wiring: the keytab-Secret existence
//! check, the rendered TOML landing in the `ConfigMap`, and the mounts
//! landing on the `StatefulSet`.

use std::{collections::BTreeMap, sync::Arc};

use assert2::{assert, check};
use base64::Engine as _;
use crabka_operator::{
    controller::{
        kafka::reconcile as reconcile_kafka, kafka_node_pool::reconcile as reconcile_pool,
    },
    crd::{
        InterBrokerKerberos, Kafka, KafkaNodePool, KafkaNodePoolSpec, KafkaSpec, KeytabSecretRef,
        Listener, ListenerAuthentication, ListenerAuthenticationGssapi, ListenerType, NodeRole,
    },
};
use http::{Method, Response};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState, build_ctx, fake_kafka_body, fake_pool_body, fake_pool_list_item,
    fake_sts_body, fixture_ctx, happy_path_rules, json_response, mock_client, not_found_body,
};

// ── fixtures ────────────────────────────────────────────────────────────────

const KEYTAB_SECRET_NAME: &str = "broker-keytab";
const KEYTAB_KEY: &str = "krb5.keytab";
const KRB5_SECRET_NAME: &str = "krb5-conf";
const KRB5_KEY: &str = "krb5.conf";

/// A plaintext internal listener with no authentication. Used as the
/// resolved inter-broker listener in tests that want the gssapi listener
/// to be a *client* listener only (so the inter-broker-gssapi guard,
/// which requires `spec.interBrokerKerberos`, doesn't fire).
fn plain_listener(name: &str, port: i32) -> Listener {
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

/// A `type: gssapi` listener with the canonical `keytabSecretRef` and a
/// single `DEFAULT` auth-to-local rule.
fn gssapi_listener(name: &str, port: i32, tls: bool) -> Listener {
    Listener {
        name: name.into(),
        port,
        type_: ListenerType::Internal,
        tls,
        authentication: Some(ListenerAuthentication::Gssapi(
            ListenerAuthenticationGssapi {
                keytab_secret_ref: KeytabSecretRef {
                    secret_name: KEYTAB_SECRET_NAME.into(),
                    key: KEYTAB_KEY.into(),
                },
                service_name: None,
                principal_to_local_rules: vec!["DEFAULT".into()],
                realm: None,
                kdc: None,
            },
        )),
        configuration: None,
        network_policy_peers: None,
    }
}

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
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// JSON body shaped like a `core/v1/Secret` with one base64-encoded data
/// key. Used as the GET response for the keytab / krb5.conf Secret read.
fn secret_body(name: &str, namespace: &str, key: &str, value: &[u8]) -> serde_json::Value {
    let b64 = base64::engine::general_purpose::STANDARD.encode(value);
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": format!("{name}-uid") },
        "type": "Opaque",
        "data": { key: b64 },
    })
}

/// Build a rule for `GET /secrets/<name>` returning the supplied body.
fn rule_get_secret(name: &str, body: &serde_json::Value) -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: format!("/secrets/{name}"),
        response: json_response(200, body),
    }
}

/// Build a rule for `GET /secrets/<name>` returning a 404.
fn rule_get_secret_404(name: &str) -> MockRule {
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

/// Trim `happy_path_rules` for the keytab failure path: when the keytab
/// Secret is absent the reconciler short-circuits (after CA convergence +
/// pool list) to a `patch_status_with_condition` (GET status + PATCH
/// status) before upserting per-broker objects. Drop the per-broker
/// keystore / `ConfigMap` / status-PATCH rules so an unconsumed rule can't
/// mask the assertion, then re-add the GET+PATCH status pair. Mirrors
/// `reconcile_oauth_introspection.rs::rules_for_failure_path`.
fn rules_for_failure_path(name: &str, namespace: &str) -> Vec<MockRule> {
    let mut rules = happy_path_rules(name, namespace, &[]);
    rules.retain(|r| {
        !r.path_substr.contains("-kafka-brokers")
            && !r.path_substr.contains("/configmaps/")
            && !r.path_substr.contains(&format!("/kafkas/{name}/status"))
    });
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
/// its `reason` matches. The keytab-Secret failure path patches the
/// `Ready` condition (per-listener validation already succeeded; the
/// failure is in the Secret-touching code that runs *after* validation),
/// exactly like the OAuth introspection missing-Secret path.
fn assert_ready_false_with_reason(
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
/// in `observed`.
fn extract_broker0_toml(observed: &[http::Request<hyper::body::Bytes>], cluster: &str) -> String {
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
    body["data"]["broker-0.toml"]
        .as_str()
        .unwrap_or_else(|| panic!("broker-0.toml missing; body = {body}"))
        .to_string()
}

// ── pool-reconcile fixtures (StatefulSet mount/env assertions) ─────────────

fn pool_cr(name: &str, namespace: &str, parent: &str, replicas: i32) -> KafkaNodePool {
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

/// Parent-Kafka body carrying a `type: gssapi` listener. When
/// `krb5 = true`, also sets `spec.krb5ConfSecretRef`. Used as the GET
/// response for the pool reconciler's parent-Kafka read so the rendered
/// pod template picks up the keytab + krb5.conf mounts.
fn parent_kafka_body_with_gssapi(name: &str, namespace: &str, krb5: bool) -> serde_json::Value {
    let mut spec = serde_json::json!({
        "kafkaVersion": "0.1.1",
        "listeners": [{
            "name": "gss",
            "port": 9092,
            "type": "internal",
            "tls": false,
            "authentication": {
                "type": "gssapi",
                "keytabSecretRef": { "secretName": KEYTAB_SECRET_NAME, "key": KEYTAB_KEY },
                "principalToLocalRules": ["DEFAULT"],
            },
        }]
    });
    if krb5 {
        spec["krb5ConfSecretRef"] =
            serde_json::json!({ "secretName": KRB5_SECRET_NAME, "key": KRB5_KEY });
    }
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": spec,
        // The pool reconciler gates broker formatting on the parent's version
        // verdict (KafkaVersionValid=True or a finalized metadata version; see
        // kafka_node_pool::version_gate), so the parent must look like a
        // validated cluster or no StatefulSet is rendered. Mirrors
        // reconcile_oauth_trust::cleared_version_status.
        "status": {
            "conditions": [{
                "type": "KafkaVersionValid",
                "status": "True",
                "reason": "Valid",
                "message": "kafkaVersion 0.1.1 metadata.version 0.1",
                "lastTransitionTime": "2026-05-22T00:00:00Z"
            }],
            "metadataVersion": "0.1"
        }
    })
}

/// FIFO rule sequence the pool reconciler needs:
///   1. GET kafkas/<parent>              → `parent_body`
///   2. GET statefulsets/<parent>-<pool> → 404 (first reconcile)
///   3. PATCH statefulsets/<parent>-<pool> (SSA)
///   4. GET statefulsets/<parent>-<pool>  (post-apply status read)
///   5. PATCH kafkanodepools/<pool>/status
fn pool_reconcile_rules(
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

fn pool_ctx(
    namespace: &str,
    rules: Vec<MockRule>,
) -> (Arc<crabka_operator::context::Context>, Arc<MockState>) {
    let state = MockState::new(rules);
    let client = mock_client(&state, namespace);
    (Arc::new(fixture_ctx(client, namespace)), state)
}

// ── test 1a: happy path — [gssapi] TOML block renders into the ConfigMap ────

/// A `type: gssapi` listener with its keytab Secret present reconciles
/// cleanly: the keytab Secret is GET'd + validated, and the broker-config
/// `ConfigMap` embeds the broker-global `[gssapi]` block with
/// `keytab_path = "/etc/crabka/gssapi-keytab/keytab"` plus the per-listener
/// `sasl_config = { enabled_mechanisms = ["GSSAPI"] }` row.
#[tokio::test]
async fn gssapi_listener_renders_gssapi_toml_block_and_mechanism() {
    let items = vec![fake_pool_list_item("brokers", "ns1", "c1", 1, 1)];
    let mut rules = happy_path_rules("c1", "ns1", &items);
    // Keytab-Secret GET fires after the OAuth checks (no-ops here) and
    // before the per-broker keystore step. FIFO substring matching makes
    // ordering among non-overlapping substrings irrelevant.
    rules.push(rule_get_secret(
        KEYTAB_SECRET_NAME,
        &secret_body(
            KEYTAB_SECRET_NAME,
            "ns1",
            KEYTAB_KEY,
            b"\x05\x02fake-keytab",
        ),
    ));

    let (ctx, state) = build_ctx("ns1", rules);
    // The gssapi listener is a *client* listener; inter-broker traffic
    // uses the plaintext `plain` listener so the inter-broker-gssapi
    // guard (which would demand `spec.interBrokerKerberos`) stays dormant.
    let mut kafka = kafka_cr(
        "c1",
        "ns1",
        vec![
            plain_listener("plain", 9091),
            gssapi_listener("gss", 9092, false),
        ],
    );
    kafka.spec.inter_broker_listener_name = Some("plain".into());
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // Keytab-Secret existence check fired.
    assert!(
        observed.iter().any(|r| {
            r.method() == Method::GET
                && r.uri()
                    .to_string()
                    .contains(&format!("/secrets/{KEYTAB_SECRET_NAME}"))
        }),
        "keytab Secret GET must fire; observed: {:?}",
        observed
            .iter()
            .map(|r| format!("{} {}", r.method(), r.uri()))
            .collect::<Vec<_>>(),
    );

    let toml = extract_broker0_toml(&observed, "c1");
    // `[inter_broker_credentials]` must be absent: the synthesized inter-broker
    // listener is not gssapi, and there is no interBrokerKerberos.
    for (needle, want) in [
        ("[gssapi]", true),
        ("keytab_path = \"/etc/crabka/gssapi-keytab/keytab\"", true),
        ("sasl_config = { enabled_mechanisms = [\"GSSAPI\"] }", true),
        ("[inter_broker_credentials]", false),
    ] {
        assert!(
            toml.contains(needle) == want,
            "needle {needle:?}, want present = {want}; TOML: {toml}"
        );
    }
}

// ── test 1b: happy path — StatefulSet mounts the gssapi-keytab volume ───────

/// The pool reconciler renders a pod template that mounts the keytab
/// Secret as a projected-items `gssapi-keytab` volume at the fixed dir
/// `/etc/crabka/gssapi-keytab`, with the user's key pinned to the item
/// path `keytab`.
#[tokio::test]
async fn gssapi_listener_statefulset_mounts_keytab_volume() {
    let rules = pool_reconcile_rules(
        "c1b",
        "brokers",
        "ns1b",
        &parent_kafka_body_with_gssapi("c1b", "ns1b", /* krb5 = */ false),
    );
    let (ctx, state) = pool_ctx("ns1b", rules);
    let pool = pool_cr("brokers", "ns1b", "c1b", 1);
    reconcile_pool(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/statefulsets/c1b-brokers")
        })
        .expect("StatefulSet PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS body is JSON");
    let pod_spec = &body["spec"]["template"]["spec"];

    // Volume sources the keytab Secret with a projected item.
    let volumes = pod_spec["volumes"].as_array().expect("volumes array");
    let kt_vol = volumes
        .iter()
        .find(|v| v["name"] == "gssapi-keytab")
        .unwrap_or_else(|| panic!("gssapi-keytab volume present; body = {body}"));
    assert!(
        kt_vol["secret"]["secretName"] == KEYTAB_SECRET_NAME,
        "keytab volume sources the user's Secret; body = {body}"
    );
    assert!(
        kt_vol["secret"]["items"] == serde_json::json!([{ "key": KEYTAB_KEY, "path": "keytab" }]),
        "exactly one projected item, pinned to the fixed `keytab` path; body = {body}"
    );

    // Broker-container volumeMount at the canonical keytab dir.
    let containers = pod_spec["containers"].as_array().expect("containers array");
    let broker = containers
        .iter()
        .find(|c| c["name"] == "broker")
        .unwrap_or_else(|| panic!("broker container present; body = {body}"));
    let mounts = broker["volumeMounts"].as_array().expect("volumeMounts");
    let kt_mount = mounts
        .iter()
        .find(|m| m["name"] == "gssapi-keytab")
        .unwrap_or_else(|| panic!("gssapi-keytab mount present; body = {body}"));
    assert!(
        kt_mount["mountPath"] == "/etc/crabka/gssapi-keytab",
        "canonical keytab mount dir; body = {body}"
    );
}

// ── test 2: missing keytab Secret → Ready=False MissingGssapiKeytabSecret ───

/// The keytab Secret is entirely absent (mock returns 404 on the
/// `get_opt`). The cluster reconciler patches the `Kafka` CR `Ready`
/// condition to `status: "False"` with reason
/// `MissingGssapiKeytabSecret`, and no broker-config `ConfigMap` PATCH
/// fires. Mirrors the OAuth introspection missing-Secret assertion
/// (`reconcile_oauth_introspection.rs::assert_ready_false_with_reason`).
#[tokio::test]
async fn gssapi_listener_missing_keytab_secret_rejects_with_missing_gssapi_keytab_secret() {
    let mut rules = rules_for_failure_path("c2", "ns2");
    rules.push(rule_get_secret_404(KEYTAB_SECRET_NAME));

    let (ctx, state) = build_ctx("ns2", rules);
    let kafka = kafka_cr("c2", "ns2", vec![gssapi_listener("gss", 9092, false)]);
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_ready_false_with_reason(&observed, "c2", "MissingGssapiKeytabSecret");

    // No ConfigMap PATCH on a failure path.
    assert!(
        !observed.iter().any(|r| r.method() == Method::PATCH
            && r.uri().to_string().contains("/configmaps/c2-broker-config")),
        "broker-config ConfigMap must not be PATCHed on MissingGssapiKeytabSecret",
    );
}

// ── test 3: inter-broker GSSAPI → [inter_broker_credentials] TOML ───────────

/// When `interBrokerListenerName` resolves to the gssapi listener AND
/// `spec.interBrokerKerberos` is set, the broker-config `ConfigMap` TOML
/// gains the `[inter_broker_credentials]` block with `type = "gssapi"`,
/// the shared client principal, and the KDC URL.
#[tokio::test]
async fn inter_broker_gssapi_renders_inter_broker_credentials_block() {
    let items = vec![fake_pool_list_item("brokers", "ns3", "c3", 1, 1)];
    let mut rules = happy_path_rules("c3", "ns3", &items);
    rules.push(rule_get_secret(
        KEYTAB_SECRET_NAME,
        &secret_body(
            KEYTAB_SECRET_NAME,
            "ns3",
            KEYTAB_KEY,
            b"\x05\x02fake-keytab",
        ),
    ));

    let (ctx, state) = build_ctx("ns3", rules);
    let mut kafka = kafka_cr("c3", "ns3", vec![gssapi_listener("gss", 9092, false)]);
    kafka.spec.inter_broker_listener_name = Some("gss".into());
    kafka.spec.inter_broker_kerberos = Some(InterBrokerKerberos {
        client_principal: "kafka@EXAMPLE.COM".into(),
        service_name: None,
        kdc_url: "tcp://kdc:88".into(),
    });
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c3");

    for needle in [
        "[inter_broker_credentials]",
        "type = \"gssapi\"",
        "client_principal = \"kafka@EXAMPLE.COM\"",
        "kdc_url = \"tcp://kdc:88\"",
    ] {
        assert!(toml.contains(needle), "missing {needle:?}; TOML: {toml}");
    }
}

// ── test 5: cross-crate contract — round-trip through the real broker parser ─

/// Closes the operator↔broker TOML contract loop end-to-end: the operator
/// renders a `[gssapi]` block (with `realm`/`kdc` set) AND an
/// `[inter_broker_credentials]` block for a gssapi listener that doubles as
/// the inter-broker listener, then we feed that *rendered* TOML straight
/// through the broker's own `FileConfig` parser + `apply_to`. This proves
/// the broker's `deny_unknown_fields` schema accepts every key the operator
/// emits and that each one maps through to the live `BrokerConfig` with the
/// right value — i.e. the wire contract is byte-exact, not just
/// string-shaped.
#[tokio::test]
async fn rendered_gssapi_toml_round_trips_through_broker_file_config() {
    let items = vec![fake_pool_list_item("brokers", "ns5", "c5", 1, 1)];
    let mut rules = happy_path_rules("c5", "ns5", &items);
    rules.push(rule_get_secret(
        KEYTAB_SECRET_NAME,
        &secret_body(
            KEYTAB_SECRET_NAME,
            "ns5",
            KEYTAB_KEY,
            b"\x05\x02fake-keytab",
        ),
    ));

    let (ctx, state) = build_ctx("ns5", rules);

    // A gssapi listener with realm/kdc set, that is ALSO the inter-broker
    // listener — exercises the [gssapi] block, the realm/kdc render
    // branches, and the [inter_broker_credentials] block in one render.
    let gss = Listener {
        name: "gss".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: false,
        authentication: Some(ListenerAuthentication::Gssapi(
            ListenerAuthenticationGssapi {
                keytab_secret_ref: KeytabSecretRef {
                    secret_name: KEYTAB_SECRET_NAME.into(),
                    key: KEYTAB_KEY.into(),
                },
                service_name: Some("kafka".into()),
                principal_to_local_rules: vec![
                    "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//".into(),
                    "DEFAULT".into(),
                ],
                realm: Some("EXAMPLE.COM".into()),
                kdc: Some("tcp://kdc:88".into()),
            },
        )),
        configuration: None,
        network_policy_peers: None,
    };

    let mut kafka = kafka_cr("c5", "ns5", vec![gss]);
    kafka.spec.inter_broker_listener_name = Some("gss".into());
    kafka.spec.inter_broker_kerberos = Some(InterBrokerKerberos {
        client_principal: "kafka@EXAMPLE.COM".into(),
        service_name: Some("kafka".into()),
        kdc_url: "tcp://kdc:88".into(),
    });
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c5");

    // Parse through the REAL broker parser, then apply to a live BrokerConfig.
    let mut fc: crabka_broker::file_config::FileConfig =
        toml::from_str(&toml).expect("broker parses operator-rendered gssapi TOML");
    // The operator now emits a `controller_quorum_voters` set of per-pod
    // headless FQDNs. `apply_to` DNS-resolves each voter (bounded retry),
    // which can't succeed against synthetic test hostnames — and this test
    // only exercises the gssapi/inter-broker-credentials render path, not
    // quorum wiring. Drop the voters so the round-trip stays hermetic.
    fc.controller_quorum_voters.clear();
    let mut bc = crabka_broker::config::BrokerConfig::default();
    fc.apply_to(&mut bc)
        .expect("apply rendered gssapi TOML to BrokerConfig");

    // [gssapi] survives the round trip with every field intact.
    let g = bc.gssapi.expect("bc.gssapi must be Some after round trip");
    check!(g.service_name == "kafka");
    check!(
        g.principal_to_local_rules.len() == 2,
        "both auth_to_local rules must parse through"
    );
    check!(g.realm == Some("EXAMPLE.COM".into()));
    check!(g.kdc == Some("tcp://kdc:88".into()));
    check!(g.keytab_path == std::path::PathBuf::from("/etc/crabka/gssapi-keytab/keytab"));

    // [inter_broker_credentials] survives as the Gssapi variant with the
    // shared client principal, service name, KDC URL, and keytab path.
    let creds = bc
        .inter_broker_credentials
        .expect("bc.inter_broker_credentials must be Some after round trip");
    assert!(
        creds
            == crabka_broker::config::InterBrokerCredentials::Gssapi {
                keytab_path: std::path::PathBuf::from("/etc/crabka/gssapi-keytab/keytab"),
                client_principal: "kafka@EXAMPLE.COM".into(),
                service_name: "kafka".into(),
                kdc_url: "tcp://kdc:88".into(),
            }
    );
}

// ── test 4: krb5.conf → krb5-conf volume/mount + KRB5_CONFIG env ────────────

/// `spec.krb5ConfSecretRef` drives a `krb5-conf` volume (sourcing the
/// user's Secret with the key pinned to `krb5.conf`), a broker-container
/// volumeMount at `/etc/crabka/krb5`, and a `KRB5_CONFIG` env pointing at
/// `/etc/crabka/krb5/krb5.conf`. Asserted via the pool reconciler's
/// rendered `StatefulSet` (the layer that mounts the volume + sets the env).
#[tokio::test]
async fn krb5_conf_statefulset_mounts_volume_and_sets_env() {
    let rules = pool_reconcile_rules(
        "c4",
        "brokers",
        "ns4",
        &parent_kafka_body_with_gssapi("c4", "ns4", /* krb5 = */ true),
    );
    let (ctx, state) = pool_ctx("ns4", rules);
    let pool = pool_cr("brokers", "ns4", "c4", 1);
    reconcile_pool(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/statefulsets/c4-brokers")
        })
        .expect("StatefulSet PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS body is JSON");
    let pod_spec = &body["spec"]["template"]["spec"];

    // krb5-conf volume sources the user's Secret with the key pinned.
    let volumes = pod_spec["volumes"].as_array().expect("volumes array");
    let krb5_vol = volumes
        .iter()
        .find(|v| v["name"] == "krb5-conf")
        .unwrap_or_else(|| panic!("krb5-conf volume present; body = {body}"));
    assert!(
        krb5_vol["secret"]["secretName"] == KRB5_SECRET_NAME,
        "krb5-conf volume sources the user's Secret; body = {body}"
    );
    let krb5_items = krb5_vol["secret"]["items"]
        .as_array()
        .unwrap_or_else(|| panic!("projected items present; body = {body}"));
    assert!(krb5_items[0]["path"] == "krb5.conf", "body = {body}");

    // Broker-container volumeMount + KRB5_CONFIG env.
    let containers = pod_spec["containers"].as_array().expect("containers array");
    let broker = containers
        .iter()
        .find(|c| c["name"] == "broker")
        .unwrap_or_else(|| panic!("broker container present; body = {body}"));
    let mounts = broker["volumeMounts"].as_array().expect("volumeMounts");
    let krb5_mount = mounts
        .iter()
        .find(|m| m["name"] == "krb5-conf")
        .unwrap_or_else(|| panic!("krb5-conf mount present; body = {body}"));
    assert!(
        krb5_mount["mountPath"] == "/etc/crabka/krb5",
        "body = {body}"
    );

    let env = broker["env"].as_array().expect("env array");
    let krb5_config = env
        .iter()
        .find(|e| e["name"] == "KRB5_CONFIG")
        .unwrap_or_else(|| panic!("KRB5_CONFIG env present; body = {body}"));
    assert!(
        krb5_config["value"] == "/etc/crabka/krb5/krb5.conf",
        "KRB5_CONFIG must point at the mounted krb5.conf; body = {body}"
    );
}
