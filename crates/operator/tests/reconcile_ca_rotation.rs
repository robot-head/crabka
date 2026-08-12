//! Integration tests for CA rotation through the full `reconcile`.
//!
//! These tests drive the reconciler against the FIFO mock. They seed
//! *existing* cluster-CA Secrets so that the rotation paths run: same-key
//! renewal and staged key replacement. They then assert the observable Secret
//! patches and the `CaRotation` condition. The
//! `controller::cluster_ca::rotation_tests` unit module covers the decision
//! logic in full. These tests verify the reconciler wiring.

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
    MockRule, build_ctx, fake_configmap_body, fake_converged_sts_body, fake_kafka_body,
    fake_keystore_secret, fake_pool_list_body, fake_pool_list_item, fake_service_body,
    json_response, not_found_body,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

/// A Secret body with base64 `data` and plain `annotations`.
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

fn get(path: String, body: &Value) -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: path,
        response: json_response(200, body),
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

fn patch(path: String, body: &Value) -> MockRule {
    MockRule {
        method: Method::PATCH,
        path_substr: path,
        response: json_response(200, body),
    }
}

fn count_cert_blocks(b64_pem: &str) -> usize {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64_pem)
        .expect("ca.crt base64");
    let pem = String::from_utf8(bytes).expect("ca.crt utf8");
    pem.matches("BEGIN CERTIFICATE").count()
}

fn decode_secret_data(body: &Value, key: &str) -> String {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(body["data"][key].as_str().expect("Secret data value"))
        .expect("Secret data base64");
    String::from_utf8(bytes).expect("Secret data UTF-8")
}

fn cert_is_signed_by(cert_pem: &str, ca_pem: &str) -> bool {
    use rustls::pki_types::{CertificateDer, pem::PemObject as _};
    use x509_parser::prelude::{FromDer as _, X509Certificate};

    let Some(Ok(cert_der)) = CertificateDer::pem_slice_iter(cert_pem.as_bytes()).next() else {
        return false;
    };
    let Ok((_, cert)) = X509Certificate::from_der(cert_der.as_ref()) else {
        return false;
    };
    let Some(Ok(ca_der)) = CertificateDer::pem_slice_iter(ca_pem.as_bytes()).next() else {
        return false;
    };
    let Ok((_, ca)) = X509Certificate::from_der(ca_der.as_ref()) else {
        return false;
    };
    cert.verify_signature(Some(ca.public_key())).is_ok()
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
            broker_tuning: None,
            gres_registry: None,
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

/// The non-CA half of the reconcile call sequence: service, cluster-id, and
/// pool LIST. It also holds the tail: keystore, CM, pool adopt, and status.
/// Each test splices the CA GET and PATCH rules in between.
fn head_rules(c: &str, ns: &str) -> Vec<MockRule> {
    head_rules_for_pool(c, ns, fake_pool_list_item("brokers", ns, c, 1, 1))
}

fn head_rules_for_pool(c: &str, ns: &str, pool: Value) -> Vec<MockRule> {
    head_rules_for_pool_and_statefulsets(c, ns, pool, &[])
}

fn head_rules_for_pool_and_statefulsets(
    c: &str,
    ns: &str,
    pool: Value,
    statefulsets: &[Value],
) -> Vec<MockRule> {
    vec![
        patch(
            format!("/services/{c}-broker-headless"),
            &fake_service_body(&format!("{c}-broker-headless"), ns),
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
            &fake_pool_list_body(&[pool]),
        ),
        get(
            format!("/namespaces/{ns}/statefulsets"),
            &json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSetList",
                "metadata": { "resourceVersion": "1" },
                "items": statefulsets,
            }),
        ),
        get(
            format!("/namespaces/{ns}/pods"),
            &json!({
                "apiVersion": "v1",
                "kind": "PodList",
                "metadata": { "resourceVersion": "1" },
                "items": [],
            }),
        ),
    ]
}

fn tls_user_list_body(name: &str, ns: &str, cluster: &str) -> Value {
    json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaUserList",
        "metadata": { "resourceVersion": "1" },
        "items": [{
            "apiVersion": "crabka.io/v1alpha1",
            "kind": "KafkaUser",
            "metadata": {
                "name": name,
                "namespace": ns,
                "uid": format!("{name}-uid"),
                "labels": { "crabka.io/cluster": cluster }
            },
            "spec": { "authentication": { "type": "tls" } }
        }]
    })
}

fn tail_rules(c: &str, ns: &str) -> Vec<MockRule> {
    vec![
        get_404(format!("/secrets/{c}-kafka-brokers")),
        patch(
            format!("/secrets/{c}-kafka-brokers"),
            &fake_keystore_secret(&format!("{c}-kafka-brokers"), ns),
        ),
        patch(
            format!("/configmaps/{c}-broker-config"),
            &fake_configmap_body(&format!("{c}-broker-config"), ns),
        ),
        patch(
            "/kafkanodepools/brokers?".to_string(),
            &shared::fake_pool_body("brokers", ns, c),
        ),
        patch(format!("/kafkas/{c}/status"), &fake_kafka_body(c, ns)),
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
            &secret_with(
                &format!("{c}-cluster-ca"),
                ns,
                &[("ca.key", &expiring.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-cluster-ca-cert"),
            &secret_with(
                &format!("{c}-cluster-ca-cert"),
                ns,
                &[("ca.crt", &expiring.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        patch(
            format!("/secrets/{c}-cluster-ca-cert"),
            &fake_keystore_secret(&format!("{c}-cluster-ca-cert"), ns),
        ),
        // clients CA: fresh → reuse, no patch.
        get(
            format!("/secrets/{c}-clients-ca"),
            &secret_with(
                &format!("{c}-clients-ca"),
                ns,
                &[("ca.key", &clients.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(
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
            &secret_with(
                &format!("{c}-cluster-ca"),
                ns,
                &[("ca.key", &fresh.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-cluster-ca-cert"),
            &secret_with(
                &format!("{c}-cluster-ca-cert"),
                ns,
                &[("ca.crt", &fresh.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        // StartKeyReplace patches the key Secret (stage *.next) THEN the cert Secret.
        patch(
            format!("/secrets/{c}-cluster-ca"),
            &secret_with(&format!("{c}-cluster-ca"), ns, &[], &[]),
        ),
        patch(
            format!("/secrets/{c}-cluster-ca-cert"),
            &fake_keystore_secret(&format!("{c}-cluster-ca-cert"), ns),
        ),
        get(
            format!("/secrets/{c}-clients-ca"),
            &secret_with(
                &format!("{c}-clients-ca"),
                ns,
                &[("ca.key", &clients.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(
                &format!("{c}-clients-ca-cert"),
                ns,
                &[("ca.crt", &clients.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        // Strip the consumed force annotation off the Kafka CR (metadata PATCH).
        patch(format!("/kafkas/{c}"), &fake_kafka_body(c, ns)),
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

// ---------------------------------------------------------------------------
// Test 3: clients-CA replacement distributes trust before leaf reissue.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn force_replace_clients_key_starts_trust_distribution() {
    let ns = "default";
    let c = "c3";
    let cluster = generate_cluster_ca("c3-cluster-ca", 365).expect("cluster CA");
    let clients = generate_clients_ca("c3-clients-ca", 365).expect("clients CA");

    let mut rules = head_rules(c, ns);
    rules.extend([
        get(
            format!("/secrets/{c}-cluster-ca"),
            &secret_with(
                &format!("{c}-cluster-ca"),
                ns,
                &[("ca.key", &cluster.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-cluster-ca-cert"),
            &secret_with(
                &format!("{c}-cluster-ca-cert"),
                ns,
                &[("ca.crt", &cluster.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca"),
            &secret_with(
                &format!("{c}-clients-ca"),
                ns,
                &[("ca.key", &clients.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(
                &format!("{c}-clients-ca-cert"),
                ns,
                &[("ca.crt", &clients.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        patch(
            format!("/secrets/{c}-clients-ca"),
            &secret_with(&format!("{c}-clients-ca"), ns, &[], &[]),
        ),
        patch(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(&format!("{c}-clients-ca-cert"), ns, &[], &[]),
        ),
        patch(format!("/kafkas/{c}"), &fake_kafka_body(c, ns)),
    ]);
    rules.extend(tail_rules(c, ns));

    let (ctx, state) = build_ctx(ns, rules);
    reconcile(
        Arc::new(kafka_cr(
            c,
            ns,
            &[("crabka.io/force-replace-clients-ca-key", "true")],
        )),
        ctx,
    )
    .await
    .expect("reconcile ok");
    let observed = state.take_observed();

    let cert_patch = observed
        .iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/secrets/{c}-clients-ca-cert"))
        })
        .expect("clients CA cert patch");
    let body: Value = serde_json::from_slice(cert_patch.body()).expect("cert patch JSON");
    assert!(count_cert_blocks(body["data"]["ca.crt"].as_str().expect("ca.crt")) == 2);
    assert!(body["metadata"]["annotations"]["crabka.io/ca-rotation-phase"] == "key-replace-trust");
    assert!(
        !observed
            .iter()
            .any(|request| request.uri().to_string().contains("/kafkausers")),
        "user certificates must not change before broker trust converges"
    );

    let status_patch = observed
        .iter()
        .rev()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains(&format!("/kafkas/{c}/status"))
        })
        .expect("status patch");
    let status: Value = serde_json::from_slice(status_patch.body()).expect("status JSON");
    let rotation = status_condition(&status, "CaRotation");
    assert!(rotation["status"] == "True");
    assert!(rotation["reason"] == "DistributingTrust");
    assert!(state.remaining_rules() == 0, "all rules consumed");
}

// ---------------------------------------------------------------------------
// Test 4: promotion reissues TLS users before recording leaf convergence.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn clients_key_promotion_reissues_users_before_marking_converged() {
    let ns = "default";
    let c = "c4";
    let user_name = "alice";
    let cluster = generate_cluster_ca("c4-cluster-ca", 365).expect("cluster CA");
    let old = generate_clients_ca("c4-clients-ca", 365).expect("old clients CA");
    let new = generate_clients_ca("c4-clients-ca", 365).expect("new clients CA");
    let old_user =
        crabka_security::ca::issue_user_cert(&old.cert_pem, &old.key_pem, user_name, 365)
            .expect("old user cert");
    let bundle = format!("{}{}", old.cert_pem, new.cert_pem);
    let mut pool = fake_pool_list_item("brokers", ns, c, 1, 1);
    pool["metadata"]["labels"]["crabka.io/config-hash"] = json!("trust-roll-complete");
    let statefulset = fake_converged_sts_body(
        &format!("{c}-brokers"),
        ns,
        c,
        "brokers",
        1,
        "trust-roll-complete",
    );

    let mut rules = head_rules_for_pool_and_statefulsets(c, ns, pool, &[statefulset]);
    rules.extend([
        get(
            format!("/secrets/{c}-cluster-ca"),
            &secret_with(
                &format!("{c}-cluster-ca"),
                ns,
                &[("ca.key", &cluster.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-cluster-ca-cert"),
            &secret_with(
                &format!("{c}-cluster-ca-cert"),
                ns,
                &[("ca.crt", &cluster.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca"),
            &secret_with(
                &format!("{c}-clients-ca"),
                ns,
                &[
                    ("ca.key", &old.key_pem),
                    ("ca.key.next", &new.key_pem),
                    ("ca.crt.next", &new.cert_pem),
                ],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(
                &format!("{c}-clients-ca-cert"),
                ns,
                &[("ca.crt", &bundle)],
                &[
                    ("crabka.io/ca-cert-generation", "0"),
                    ("crabka.io/ca-rotation-phase", "key-replace-trust"),
                ],
            ),
        ),
        patch(
            format!("/secrets/{c}-clients-ca"),
            &secret_with(&format!("{c}-clients-ca"), ns, &[], &[]),
        ),
        patch(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(&format!("{c}-clients-ca-cert"), ns, &[], &[]),
        ),
        get(
            format!("/namespaces/{ns}/kafkausers"),
            &tls_user_list_body(user_name, ns, c),
        ),
        get(
            format!("/secrets/{user_name}"),
            &secret_with(
                user_name,
                ns,
                &[
                    ("user.crt", &old_user.cert_pem),
                    ("user.key", &old_user.key_pem),
                    ("ca.crt", &old.cert_pem),
                ],
                &[],
            ),
        ),
        patch(
            format!("/secrets/{user_name}"),
            &secret_with(user_name, ns, &[], &[]),
        ),
        patch(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(&format!("{c}-clients-ca-cert"), ns, &[], &[]),
        ),
    ]);
    rules.extend(tail_rules(c, ns));

    let (ctx, state) = build_ctx(ns, rules);
    reconcile(Arc::new(kafka_cr(c, ns, &[])), ctx)
        .await
        .expect("reconcile ok");
    let observed = state.take_observed();

    let user_patch_index = observed
        .iter()
        .position(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/secrets/{user_name}"))
        })
        .expect("user Secret patch");
    let promoted_key_patch = observed
        .iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/secrets/{c}-clients-ca"))
                && !request.uri().to_string().contains("clients-ca-cert")
        })
        .expect("promoted clients CA key patch");
    let promoted_key_body: Value =
        serde_json::from_slice(promoted_key_patch.body()).expect("key patch JSON");
    for key in ["ca.key", "ca.key.next", "ca.crt.next"] {
        assert!(
            promoted_key_body["data"][key].is_string(),
            "staged material must survive promotion until prune: {promoted_key_body}"
        );
    }
    let cert_patch_indices: Vec<usize> = observed
        .iter()
        .enumerate()
        .filter(|(_, request)| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/secrets/{c}-clients-ca-cert"))
        })
        .map(|(index, _)| index)
        .collect();
    assert!(
        cert_patch_indices.len() == 2,
        "promote patch then convergence marker"
    );
    assert!(cert_patch_indices[0] < user_patch_index);
    assert!(user_patch_index < cert_patch_indices[1]);

    let user_body: Value =
        serde_json::from_slice(observed[user_patch_index].body()).expect("user patch JSON");
    let issued_user_cert = decode_secret_data(&user_body, "user.crt");
    assert!(cert_is_signed_by(&issued_user_cert, &new.cert_pem));
    assert!(!cert_is_signed_by(&issued_user_cert, &old.cert_pem));
    assert!(
        count_cert_blocks(user_body["data"]["ca.crt"].as_str().expect("user ca.crt")) == 2,
        "user Secret carries both trust anchors during promotion"
    );
    let marker_body: Value =
        serde_json::from_slice(observed[cert_patch_indices[1]].body()).expect("marker patch JSON");
    assert!(marker_body["metadata"]["annotations"]["crabka.io/ca-leafs-key-generation"] == "1");
    assert!(state.remaining_rules() == 0, "all rules consumed");
}

// ---------------------------------------------------------------------------
// Test 5: prune converges every managed TLS-user trust bundle in one pass.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn clients_ca_prune_removes_old_root_from_user_secrets_before_returning() {
    let ns = "default";
    let c = "c5";
    let user_name = "alice";
    let cluster = generate_cluster_ca("c5-cluster-ca", 365).expect("cluster CA");
    let old = generate_clients_ca("c5-clients-ca", 365).expect("old clients CA");
    let new = generate_clients_ca("c5-clients-ca", 365).expect("new clients CA");
    let user = crabka_security::ca::issue_user_cert(&new.cert_pem, &new.key_pem, user_name, 365)
        .expect("new-key user cert");
    let bundle = format!("{}{}", new.cert_pem, old.cert_pem);
    let mut pool = fake_pool_list_item("brokers", ns, c, 1, 1);
    pool["metadata"]["labels"]["crabka.io/config-hash"] = json!("promote-roll-complete");
    let statefulset = fake_converged_sts_body(
        &format!("{c}-brokers"),
        ns,
        c,
        "brokers",
        1,
        "promote-roll-complete",
    );

    let mut rules = head_rules_for_pool_and_statefulsets(c, ns, pool, &[statefulset]);
    rules.extend([
        get(
            format!("/secrets/{c}-cluster-ca"),
            &secret_with(
                &format!("{c}-cluster-ca"),
                ns,
                &[("ca.key", &cluster.key_pem)],
                &[("crabka.io/ca-key-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-cluster-ca-cert"),
            &secret_with(
                &format!("{c}-cluster-ca-cert"),
                ns,
                &[("ca.crt", &cluster.cert_pem)],
                &[("crabka.io/ca-cert-generation", "0")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca"),
            &secret_with(
                &format!("{c}-clients-ca"),
                ns,
                &[("ca.key", &new.key_pem)],
                &[("crabka.io/ca-key-generation", "1")],
            ),
        ),
        get(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(
                &format!("{c}-clients-ca-cert"),
                ns,
                &[("ca.crt", &bundle)],
                &[
                    ("crabka.io/ca-cert-generation", "1"),
                    ("crabka.io/ca-key-generation", "1"),
                    ("crabka.io/ca-leafs-key-generation", "1"),
                    ("crabka.io/ca-rotation-phase", "key-replace-promote"),
                ],
            ),
        ),
        patch(
            format!("/secrets/{c}-clients-ca"),
            &secret_with(&format!("{c}-clients-ca"), ns, &[], &[]),
        ),
        patch(
            format!("/secrets/{c}-clients-ca-cert"),
            &secret_with(&format!("{c}-clients-ca-cert"), ns, &[], &[]),
        ),
        get(
            format!("/namespaces/{ns}/kafkausers"),
            &tls_user_list_body(user_name, ns, c),
        ),
        get(
            format!("/secrets/{user_name}"),
            &secret_with(
                user_name,
                ns,
                &[
                    ("user.crt", &user.cert_pem),
                    ("user.key", &user.key_pem),
                    ("ca.crt", &bundle),
                ],
                &[],
            ),
        ),
        patch(
            format!("/secrets/{user_name}"),
            &secret_with(user_name, ns, &[], &[]),
        ),
    ]);
    rules.extend(tail_rules(c, ns));

    let (ctx, state) = build_ctx(ns, rules);
    reconcile(Arc::new(kafka_cr(c, ns, &[])), ctx)
        .await
        .expect("prune reconcile ok");
    let observed = state.take_observed();

    let clients_ca_patch_index = observed
        .iter()
        .position(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/secrets/{c}-clients-ca-cert"))
        })
        .expect("clients CA prune patch");
    let user_patch_index = observed
        .iter()
        .position(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/secrets/{user_name}"))
        })
        .expect("user trust patch");
    assert!(clients_ca_patch_index < user_patch_index);
    let user_patch: Value =
        serde_json::from_slice(observed[user_patch_index].body()).expect("user patch JSON");
    assert!(count_cert_blocks(user_patch["data"]["ca.crt"].as_str().expect("ca.crt")) == 1);
    assert!(decode_secret_data(&user_patch, "ca.crt") == new.cert_pem);
    assert!(state.remaining_rules() == 0, "all rules consumed");
}
