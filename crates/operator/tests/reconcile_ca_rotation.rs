//! Integration tests for CA rotation via the full `reconcile`.
//!
//! These drive the reconciler against the FIFO mock with *existing* cluster-CA
//! Secrets seeded so the rotation paths (same-key renewal, staged key
//! replacement) actually fire, and assert the observable Secret patches +
//! `CaRotation` condition. The pure decision logic is covered exhaustively by
//! the `controller::cluster_ca::rotation_tests` unit module; here we verify the
//! reconciler wiring.
#![allow(clippy::needless_pass_by_value, clippy::too_many_lines)]

use assert2::{assert, check};
#[path = "shared/mod.rs"]
mod shared;

use std::sync::Arc;

use base64::Engine as _;
use crabka_operator::{
    controller::kafka::reconcile,
    crd::{Kafka, KafkaSpec},
};
use crabka_security::ca::{generate_clients_ca, generate_cluster_ca};
use http::{Method, Response};
use serde_json::{Value, json};
use shared::{
    MockRule, build_ctx, fake_configmap_body, fake_kafka_body, fake_keystore_secret,
    fake_pool_list_body, fake_pool_list_item, fake_service_body, json_response, not_found_body,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

/// A Secret body with base64 `data` + plain `annotations`.
fn secret_with(name: &str, ns: &str, data: &[(&str, &str)], anns: &[(&str, &str)]) -> Value {
    let data_map: serde_json::Map<String, Value> = data
        .iter()
        .map(|(k, v)| ((*k).to_string(), json!(b64(v))))
        .collect();
    let ann_map: serde_json::Map<String, Value> = anns
        .iter()
        .map(|(k, v)| ((*k).to_string(), json!(*v)))
        .collect();
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": ns, "uid": format!("{name}-uid"), "annotations": ann_map },
        "type": "Opaque",
        "data": data_map,
    })
}

fn get(path: String, body: Value) -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: path,
        response: json_response(200, &body),
    }
}

fn get_404(path: String) -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: path,
        response: Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(not_found_body("not found"))
            .expect("404 builds"),
    }
}

fn patch(path: String, body: Value) -> MockRule {
    MockRule {
        method: Method::PATCH,
        path_substr: path,
        response: json_response(200, &body),
    }
}

fn count_cert_blocks(b64_pem: &str) -> usize {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64_pem)
        .expect("ca.crt base64");
    let pem = String::from_utf8(bytes).expect("ca.crt utf8");
    pem.matches("BEGIN CERTIFICATE").count()
}

fn kafka_cr(name: &str, ns: &str, anns: &[(&str, &str)]) -> Kafka {
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
    k.metadata.namespace = Some(ns.into());
    k.metadata.uid = Some(format!("{name}-uid"));
    if !anns.is_empty() {
        k.metadata.annotations = Some(
            anns.iter()
                .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
                .collect(),
        );
    }
    k
}

/// The non-CA half of the reconcile call sequence (service, cluster-id,
/// pool LIST) plus the tail (keystore, CM, pool adopt, status). The CA GET/PATCH
/// rules are spliced in between by each test.
fn head_rules(c: &str, ns: &str) -> Vec<MockRule> {
    vec![
        patch(
            format!("/services/{c}-broker-headless"),
            fake_service_body(&format!("{c}-broker-headless"), ns),
        ),
        get_404(format!("/secrets/{c}-cluster-id")),
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(
                201,
                &shared::fake_secret_body(
                    &format!("{c}-cluster-id"),
                    ns,
                    "00000000-0000-0000-0000-000000000000",
                ),
            ),
        },
        get(
            format!("/namespaces/{ns}/kafkanodepools"),
            fake_pool_list_body(&[fake_pool_list_item("brokers", ns, c, 1, 1)]),
        ),
    ]
}

fn tail_rules(c: &str, ns: &str) -> Vec<MockRule> {
    vec![
        get_404(format!("/secrets/{c}-kafka-brokers")),
        patch(
            format!("/secrets/{c}-kafka-brokers"),
            fake_keystore_secret(&format!("{c}-kafka-brokers"), ns),
        ),
        patch(
            format!("/configmaps/{c}-broker-config"),
            fake_configmap_body(&format!("{c}-broker-config"), ns),
        ),
        patch(
            "/kafkanodepools/brokers?".to_string(),
            shared::fake_pool_body("brokers", ns, c),
        ),
        patch(format!("/kafkas/{c}/status"), fake_kafka_body(c, ns)),
    ]
}

fn status_condition<'a>(body: &'a Value, type_: &str) -> &'a Value {
    body["status"]["conditions"]
        .as_array()
        .expect("conditions array")
        .iter()
        .find(|cnd| cnd["type"] == type_)
        .unwrap_or_else(|| panic!("{type_} condition missing; body = {body}"))
}

// ---------------------------------------------------------------------------
// Test 1: cluster CA within renewalDays auto-renews (same key) on reconcile.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cluster_ca_within_renewal_window_renews_same_key() {
    let ns = "default";
    let c = "c1";
    // Cluster CA with 20 days left (< 30-day default renewal) → due.
    let expiring = generate_cluster_ca("c1-cluster-ca", 20).expect("cluster CA");
    let clients = generate_clients_ca("c1-clients-ca", 365).expect("clients CA");

    let mut rules = head_rules(c, ns);
    rules.extend([
        // cluster CA: GET key, GET cert (existing, gen 0), then PATCH cert (renewal).
        get(
            format!("/secrets/{c}-cluster-ca"),
            secret_with(
                &format!("{c}-cluster-ca"),
                ns,
                &[("ca.key", &expiring.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-cluster-ca-cert"),
            secret_with(
                &format!("{c}-cluster-ca-cert"),
                ns,
                &[("ca.crt", &expiring.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        patch(
            format!("/secrets/{c}-cluster-ca-cert"),
            fake_keystore_secret(&format!("{c}-cluster-ca-cert"), ns),
        ),
        // clients CA: fresh → reuse, no patch.
        get(
            format!("/secrets/{c}-clients-ca"),
            secret_with(
                &format!("{c}-clients-ca"),
                ns,
                &[("ca.key", &clients.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca-cert"),
            secret_with(
                &format!("{c}-clients-ca-cert"),
                ns,
                &[("ca.crt", &clients.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
    ]);
    rules.extend(tail_rules(c, ns));

    let (ctx, state) = build_ctx(ns, rules);
    reconcile(Arc::new(kafka_cr(c, ns, &[])), ctx)
        .await
        .expect("reconcile ok");
    let observed = state.take_observed();

    // The cluster-ca-cert PATCH must carry a 2-block bundle (new + old) and bump
    // the cert generation to 1.
    let cert_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/secrets/{c}-cluster-ca-cert"))
        })
        .expect("cluster-ca-cert PATCH");
    let body: Value = serde_json::from_slice(cert_patch.body()).expect("cert PATCH JSON");
    let bundle_b64 = body["data"]["ca.crt"].as_str().expect("ca.crt data");
    check!(
        count_cert_blocks(bundle_b64) == 2,
        "renewal must leave the old cert in the bundle until it expires; body = {body}"
    );
    check!(
        body["metadata"]["annotations"]["crabka.io/ca-cert-generation"] == "1",
        "cert generation must bump on renewal; body = {body}"
    );

    // The clients-ca-cert must NOT be patched (fresh).
    check!(
        !observed.iter().any(|r| r.method() == Method::PATCH
            && r.uri()
                .to_string()
                .contains(&format!("/secrets/{c}-clients-ca-cert"))),
        "fresh clients CA must not be patched"
    );

    // CaRotation=True/RenewingCert in the status PATCH.
    let status_patch = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains(&format!("/kafkas/{c}/status"))
        })
        .expect("status PATCH");
    let sbody: Value = serde_json::from_slice(status_patch.body()).expect("status JSON");
    let rot = status_condition(&sbody, "CaRotation");
    check!(rot["status"] == "True", "body = {sbody}");
    check!(rot["reason"] == "RenewingCert", "body = {sbody}");

    check!(state.remaining_rules() == 0, "all rules consumed");
}

// ---------------------------------------------------------------------------
// Test 2: force-replace-ca-key starts the staged key-replacement (phase 1).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn force_replace_key_starts_staged_rotation() {
    let ns = "default";
    let c = "c2";
    // Fresh CA (not due) — only the force annotation triggers rotation.
    let fresh = generate_cluster_ca("c2-cluster-ca", 365).expect("cluster CA");
    let clients = generate_clients_ca("c2-clients-ca", 365).expect("clients CA");

    let mut rules = head_rules(c, ns);
    rules.extend([
        get(
            format!("/secrets/{c}-cluster-ca"),
            secret_with(
                &format!("{c}-cluster-ca"),
                ns,
                &[("ca.key", &fresh.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-cluster-ca-cert"),
            secret_with(
                &format!("{c}-cluster-ca-cert"),
                ns,
                &[("ca.crt", &fresh.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        // StartKeyReplace patches the key Secret (stage *.next) THEN the cert Secret.
        patch(
            format!("/secrets/{c}-cluster-ca"),
            secret_with(&format!("{c}-cluster-ca"), ns, &[], &[]),
        ),
        patch(
            format!("/secrets/{c}-cluster-ca-cert"),
            fake_keystore_secret(&format!("{c}-cluster-ca-cert"), ns),
        ),
        get(
            format!("/secrets/{c}-clients-ca"),
            secret_with(
                &format!("{c}-clients-ca"),
                ns,
                &[("ca.key", &clients.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca-cert"),
            secret_with(
                &format!("{c}-clients-ca-cert"),
                ns,
                &[("ca.crt", &clients.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        // Strip the consumed force annotation off the Kafka CR (metadata PATCH).
        patch(format!("/kafkas/{c}"), fake_kafka_body(c, ns)),
    ]);
    rules.extend(tail_rules(c, ns));

    let (ctx, state) = build_ctx(ns, rules);
    reconcile(
        Arc::new(kafka_cr(
            c,
            ns,
            &[("crabka.io/force-replace-ca-key", "true")],
        )),
        ctx,
    )
    .await
    .expect("reconcile ok");
    let observed = state.take_observed();

    // Key Secret PATCH must stage ca.key.next + ca.crt.next while keeping ca.key.
    let key_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/secrets/{c}-cluster-ca"))
                && !r.uri().to_string().contains("cluster-ca-cert")
        })
        .expect("cluster-ca key PATCH");
    let kbody: Value = serde_json::from_slice(key_patch.body()).expect("key PATCH JSON");
    for k in ["ca.key", "ca.key.next", "ca.crt.next"] {
        assert!(
            kbody["data"][k].is_string(),
            "key Secret PATCH must carry {k}; body = {kbody}"
        );
    }

    // Cert Secret PATCH must hold a 2-block trust bundle + the trust phase.
    let cert_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/secrets/{c}-cluster-ca-cert"))
        })
        .expect("cluster-ca-cert PATCH");
    let cbody: Value = serde_json::from_slice(cert_patch.body()).expect("cert PATCH JSON");
    assert!(
        count_cert_blocks(cbody["data"]["ca.crt"].as_str().expect("ca.crt")) == 2,
        "trust bundle must grow to old+new; body = {cbody}"
    );
    assert!(
        cbody["metadata"]["annotations"]["crabka.io/ca-rotation-phase"] == "key-replace-trust",
        "phase must be key-replace-trust; body = {cbody}"
    );

    // The force annotation must be stripped (null) via a metadata PATCH.
    let meta_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains(&format!("/kafkas/{c}"))
                && !r.uri().to_string().contains("/status")
        })
        .expect("metadata PATCH");
    let mbody: Value = serde_json::from_slice(meta_patch.body()).expect("meta PATCH JSON");
    assert!(
        mbody["metadata"]["annotations"]["crabka.io/force-replace-ca-key"].is_null(),
        "force annotation must be stripped; body = {mbody}"
    );

    // CaRotation=True/DistributingTrust.
    let status_patch = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains(&format!("/kafkas/{c}/status"))
        })
        .expect("status PATCH");
    let sbody: Value = serde_json::from_slice(status_patch.body()).expect("status JSON");
    let rot = status_condition(&sbody, "CaRotation");
    check!(rot["status"] == "True", "body = {sbody}");
    check!(rot["reason"] == "DistributingTrust", "body = {sbody}");

    check!(state.remaining_rules() == 0, "all rules consumed");
}
