//! Integration tests for CA + broker-keystore reconciliation.
//!
//! Test list:
//!   1. `default_flow_creates_cluster_ca_clients_ca_and_broker_keystore`
//!   2. `broker_leaf_certs_chain_to_cluster_ca`
//!   3. `scale_up_adds_entries_does_not_reissue_existing`
//!   4. `scale_down_prunes_entries`
//!   5. `byo_mode_adopts_pre_existing_secrets_does_not_overwrite`
//!   6. `byo_mode_without_pre_existing_secrets_errors_gracefully`
//!   7. `reconciler_does_not_renew_valid_leaf_certs`

use assert2::{assert, check};
#[path = "shared/mod.rs"]
mod shared;

use std::sync::Arc;

use base64::Engine as _;
use crabka_operator::{
    controller::{cluster_ca::compute_san_digest, kafka::reconcile},
    crd::{CertificateAuthority, Kafka, KafkaSpec},
};
use http::{Method, Response};
use serde_json::json;
use shared::{
    MockRule, build_ctx, fake_ca_secret, fake_configmap_body, fake_kafka_body,
    fake_keystore_secret, fake_pool_list_body, fake_service_body, json_response, not_found_body,
};
use x509_parser::pem::parse_x509_pem;

// ---------------------------------------------------------------------------
// CR + context builders
// ---------------------------------------------------------------------------

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
            broker_tuning: None,
            gres_registry: None,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

fn kafka_cr_byo(name: &str, namespace: &str) -> Kafka {
    let byo = CertificateAuthority {
        generate_certificate_authority: false,
        validity_days: 365,
        renewal_days: 30,
    };
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
            cluster_ca: Some(byo.clone()),
            clients_ca: Some(byo),
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
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

// build_ctx, fake_ca_secret, fake_keystore_secret are in shared/mod.rs.

// ---------------------------------------------------------------------------
// Local mock-body helpers
// ---------------------------------------------------------------------------

/// Minimal `Secret` body with ca.key and ca.crt populated (base64-encoded
/// PEM content). Used for BYO tests where the operator must read existing
/// CA material and re-use it.
fn fake_ca_secret_with_pem(name: &str, namespace: &str, pem: &str) -> serde_json::Value {
    let b64 = base64::engine::general_purpose::STANDARD.encode(pem.as_bytes());
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "byo-ca-uid" },
        "type": "Opaque",
        "data": { "ca.crt": b64, "ca.key": b64 }
    })
}

/// Rule list for the no-op CA path (both GET -> existing Secret with PEM data,
/// no PATCH needed for CA Secrets). The caller is responsible for injecting the
/// appropriate GET responses (real PEM or 404).
fn secret_rule_404(method: Method, path_substr: String) -> MockRule {
    MockRule {
        method,
        path_substr,
        response: Response::builder()
            .status(404)
            .header("content-type", "application/json")
            .body(not_found_body("not found"))
            .expect("404 builds"),
    }
}

fn fake_secret_body_cluster_id(name: &str, namespace: &str) -> serde_json::Value {
    let b64 =
        base64::engine::general_purpose::STANDARD.encode(b"00000000-0000-0000-0000-000000000000");
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "secret-uid" },
        "type": "Opaque",
        "data": { "clusterId": b64 },
    })
}

// ---------------------------------------------------------------------------
// Test 1: Default flow creates 5 CA-related Secrets
//
// On a fresh cluster:
//   - cluster-ca key + cert written (2 PATCH)
//   - clients-ca key + cert written (2 PATCH)
//   - kafka-brokers keystore written (1 PATCH)
// Total: 5 CA-related PATCHes (GET → 404 for each pair first)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn default_flow_creates_cluster_ca_clients_ca_and_broker_keystore() {
    let ns = "ns1";
    let name = "c1";
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let clients_ca_key = format!("{name}-clients-ca");
    let clients_ca_cert = format!("{name}-clients-ca-cert");
    let keystore_name = format!("{name}-kafka-brokers");
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");

    let rules = vec![
        // 1. PATCH headless service
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, ns)),
        },
        // 2. GET cluster-id secret → 404
        secret_rule_404(Method::GET, format!("/secrets/{secret_name}")),
        // 3. POST cluster-id secret → 201
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(201, &fake_secret_body_cluster_id(&secret_name, ns)),
        },
        // 4. GET cluster-ca key → 404
        secret_rule_404(Method::GET, format!("/secrets/{cluster_ca_key}")),
        // 5. GET cluster-ca cert → 404
        secret_rule_404(Method::GET, format!("/secrets/{cluster_ca_cert}")),
        // 6. PATCH cluster-ca key
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_key, ns)),
        },
        // 7. PATCH cluster-ca cert
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_cert, ns)),
        },
        // 8. GET clients-ca key → 404
        secret_rule_404(Method::GET, format!("/secrets/{clients_ca_key}")),
        // 9. GET clients-ca cert → 404
        secret_rule_404(Method::GET, format!("/secrets/{clients_ca_cert}")),
        // 10. PATCH clients-ca key
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_key, ns)),
        },
        // 11. PATCH clients-ca cert
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_cert, ns)),
        },
        // 12. GET kafkanodepools list (empty)
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(&[])),
        },
        // 13. GET keystore → 404 (no pools → no brokers → keystore still called with empty requests)
        secret_rule_404(Method::GET, format!("/secrets/{keystore_name}")),
        // 14. PATCH keystore
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &fake_keystore_secret(&keystore_name, ns)),
        },
        // 15. PATCH configmap
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, ns)),
        },
        // 16. PATCH kafka status
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkas/{name}/status"),
            response: json_response(200, &fake_kafka_body(name, ns)),
        },
    ];

    let (ctx, state) = build_ctx(ns, rules);
    let kafka = kafka_cr(name, ns);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let methods_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    // 5 CA-related PATCHes: cluster-ca key, cluster-ca cert, clients-ca key, clients-ca cert, keystore.
    let ca_patches: Vec<_> = methods_uris
        .iter()
        .filter(|(m, u)| {
            *m == Method::PATCH
                && (u.contains("-cluster-ca")
                    || u.contains("-clients-ca")
                    || u.contains("-kafka-brokers"))
        })
        .collect();
    assert!(
        ca_patches.len() == 5,
        "expected 5 CA-related PATCH calls (2 cluster-ca, 2 clients-ca, 1 keystore), \
         got {}: {:?}",
        ca_patches.len(),
        ca_patches
    );

    // cluster-ca key + cert, clients-ca key + cert, and broker keystore
    // PATCHes must all be present.
    for target in [
        &cluster_ca_key,
        &cluster_ca_cert,
        &clients_ca_key,
        &clients_ca_cert,
        &keystore_name,
    ] {
        assert!(
            methods_uris
                .iter()
                .any(|(m, u)| *m == Method::PATCH && u.contains(target.as_str())),
            "PATCH for {target} must be present",
        );
    }

    // Status PATCH lands with ClusterCaReady + ClientsCaReady conditions.
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkas/{name}/status"))
        })
        .expect("status PATCH must be present");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");
    let cluster_ca_cond = conds
        .iter()
        .find(|c| c["type"] == "ClusterCaReady")
        .unwrap_or_else(|| panic!("ClusterCaReady condition missing, body = {body}"));
    assert!(cluster_ca_cond["status"] == "True", "body = {body}");
    assert!(cluster_ca_cond["reason"] == "CaReady", "body = {body}");
    let clients_ca_cond = conds
        .iter()
        .find(|c| c["type"] == "ClientsCaReady")
        .unwrap_or_else(|| panic!("ClientsCaReady condition missing, body = {body}"));
    check!(clients_ca_cond["status"] == "True", "body = {body}");
    check!(clients_ca_cond["reason"] == "CaReady", "body = {body}");

    check!(
        state.remaining_rules() == 0,
        "all preloaded rules must have been consumed"
    );
}

// ---------------------------------------------------------------------------
// Test 5: BYO mode adopts pre-existing Secrets, doesn't overwrite them
//
// The operator must NOT PATCH the CA Secrets when they already exist.
// It must still PATCH the broker keystore (signed against the BYO CA).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn byo_mode_adopts_pre_existing_secrets_does_not_overwrite() {
    let ns = "ns5";
    let name = "c5";
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let clients_ca_key = format!("{name}-clients-ca");
    let clients_ca_cert = format!("{name}-clients-ca-cert");
    let keystore_name = format!("{name}-kafka-brokers");
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");

    // Generate real CA material so the operator can parse the PEM and derive
    // notAfter for the status field.
    let cluster_ca_mat =
        crabka_security::ca::generate_cluster_ca("c5-cluster-ca", 365).expect("cluster CA gen");
    let clients_ca_mat =
        crabka_security::ca::generate_clients_ca("c5-clients-ca", 365).expect("clients CA gen");

    let rules = vec![
        // 1. PATCH headless service
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, ns)),
        },
        // 2. GET cluster-id → 404
        secret_rule_404(Method::GET, format!("/secrets/{secret_name}")),
        // 3. POST cluster-id → 201
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(201, &fake_secret_body_cluster_id(&secret_name, ns)),
        },
        // 4. GET cluster-ca key → 200 with real PEM (BYO pre-seeded)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_key, ns, &cluster_ca_mat.key_pem),
            ),
        },
        // 5. GET cluster-ca cert → 200 with real PEM
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_cert, ns, &cluster_ca_mat.cert_pem),
            ),
        },
        // NOTE: No PATCH for cluster-ca key or cert — the operator must not overwrite them.
        // 6. GET clients-ca key → 200 with real PEM
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_key, ns, &clients_ca_mat.key_pem),
            ),
        },
        // 7. GET clients-ca cert → 200 with real PEM
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_cert, ns, &clients_ca_mat.cert_pem),
            ),
        },
        // NOTE: No PATCH for clients-ca key or cert — the operator must not overwrite them.
        // 8. GET kafkanodepools list (empty)
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(&[])),
        },
        // 9. GET keystore → 404 (first time)
        secret_rule_404(Method::GET, format!("/secrets/{keystore_name}")),
        // 10. PATCH keystore (signed against the BYO cluster CA)
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &fake_keystore_secret(&keystore_name, ns)),
        },
        // 11. PATCH configmap
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, ns)),
        },
        // 12. PATCH kafka status
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkas/{name}/status"),
            response: json_response(200, &fake_kafka_body(name, ns)),
        },
    ];

    let (ctx, state) = build_ctx(ns, rules);
    let kafka = kafka_cr_byo(name, ns);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let methods_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    // The operator must NOT have patched the CA Secrets.
    for suffix in &[
        cluster_ca_key.as_str(),
        cluster_ca_cert.as_str(),
        clients_ca_key.as_str(),
        clients_ca_cert.as_str(),
    ] {
        let ca_patches: Vec<_> = methods_uris
            .iter()
            .filter(|(m, u)| *m == Method::PATCH && u.contains(suffix))
            .collect();
        assert!(
            ca_patches.is_empty(),
            "BYO mode: operator must not PATCH CA Secret {suffix}, \
             got {}: {:?}",
            ca_patches.len(),
            ca_patches,
        );
    }

    // The operator MUST have patched the broker keystore.
    assert!(
        methods_uris
            .iter()
            .any(|(m, u)| *m == Method::PATCH && u.contains(&keystore_name)),
        "BYO mode: broker keystore PATCH must still happen",
    );

    // Status conditions: ClusterCaReady=True + ClientsCaReady=True (BYO Secrets were present + parseable).
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkas/{name}/status"))
        })
        .expect("status PATCH must be present");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");
    let cluster_ca_cond = conds
        .iter()
        .find(|c| c["type"] == "ClusterCaReady")
        .unwrap_or_else(|| panic!("ClusterCaReady condition missing, body = {body}"));
    assert!(
        cluster_ca_cond["status"] == "True",
        "BYO present: ClusterCaReady must be True, body = {body}"
    );
    let clients_ca_cond = conds
        .iter()
        .find(|c| c["type"] == "ClientsCaReady")
        .unwrap_or_else(|| panic!("ClientsCaReady condition missing, body = {body}"));
    assert!(
        clients_ca_cond["status"] == "True",
        "BYO present: ClientsCaReady must be True, body = {body}"
    );

    assert!(
        state.remaining_rules() == 0,
        "all preloaded rules must have been consumed"
    );
}

// ---------------------------------------------------------------------------
// Test 6: BYO mode without pre-existing Secrets errors gracefully
//
// When generateCertificateAuthority=false and the CA Secret pair is absent:
//   - reconcile must NOT return Err (it returns Ok(Action::requeue))
//   - status is patched with ClusterCaReady=False reason=ByoCaMissing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn byo_mode_without_pre_existing_secrets_errors_gracefully() {
    let ns = "ns6";
    let name = "c6";
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let svc_name = format!("{name}-broker-headless");
    let secret_name = format!("{name}-cluster-id");

    // When the cluster-ca key Secret is missing and BYO is set, the
    // reconciler calls patch_status_with_condition which does:
    //   1. GET kafkas/<name>/status  (to read existing conditions)
    //   2. PATCH kafkas/<name>/status
    // then returns Ok(Action::requeue).

    let rules = vec![
        // 1. PATCH headless service
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, ns)),
        },
        // 2. GET cluster-id → 404
        secret_rule_404(Method::GET, format!("/secrets/{secret_name}")),
        // 3. POST cluster-id → 201
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(201, &fake_secret_body_cluster_id(&secret_name, ns)),
        },
        // 3b. Pools are listed before the CA reconcile (rollout
        // convergence check), so the BYO-missing early-out sees a pool LIST.
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(&[])),
        },
        // 4. GET cluster-ca key → 404 (BYO Secret missing)
        secret_rule_404(Method::GET, format!("/secrets/{cluster_ca_key}")),
        // 5. GET cluster-ca cert → 404 (BYO Secret missing)
        secret_rule_404(Method::GET, format!("/secrets/{cluster_ca_cert}")),
        // 6. GET kafka status (patch_status_with_condition reads current conditions)
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{name}/status"),
            response: json_response(200, &fake_kafka_body(name, ns)),
        },
        // 7. PATCH kafka status with ClusterCaReady=False
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkas/{name}/status"),
            response: json_response(200, &fake_kafka_body(name, ns)),
        },
    ];

    let (ctx, state) = build_ctx(ns, rules);
    let kafka = kafka_cr_byo(name, ns);

    // Reconcile must NOT return an Err — BYO-missing is a graceful condition,
    // not a hard failure. The reconciler requeues after patching the status.
    let result = reconcile(Arc::new(kafka), ctx).await;
    assert!(
        result.is_ok(),
        "BYO-missing must return Ok(requeue), got: {result:?}",
    );

    let observed = state.take_observed();
    let methods_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    // No CA PATCH must have happened (operator gave up before writing).
    for ca_suffix in &[
        cluster_ca_key.as_str(),
        cluster_ca_cert.as_str(),
        &format!("{name}-clients-ca"),
        &format!("{name}-clients-ca-cert"),
    ] {
        assert!(
            !methods_uris
                .iter()
                .any(|(m, u)| *m == Method::PATCH && u.contains(ca_suffix)),
            "BYO-missing: operator must not PATCH CA Secret {ca_suffix}",
        );
    }

    // The status PATCH body must carry ClusterCaReady=False reason=ByoCaMissing.
    let status_patch = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkas/{name}/status"))
        })
        .last() // take the last one in case there's a GET+PATCH pair
        .expect("status PATCH must be present");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");
    let cluster_ca_cond = conds
        .iter()
        .find(|c| c["type"] == "ClusterCaReady")
        .unwrap_or_else(|| panic!("ClusterCaReady condition missing, body = {body}"));
    check!(
        cluster_ca_cond["status"] == "False",
        "ByoCaMissing: ClusterCaReady must be False, body = {body}"
    );
    check!(
        cluster_ca_cond["reason"] == "ByoCaMissing",
        "ByoCaMissing: reason must be ByoCaMissing, body = {body}"
    );

    check!(
        state.remaining_rules() == 0,
        "all preloaded rules must have been consumed"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Reconciler does NOT renew valid leaf certs
//
// `ensure_broker_keystore` reuses an existing cert when both `<id>.crt` and
// `<id>.key` are present in the keystore Secret, regardless of the cert's
// `notAfter`. Only the CronJob (`ca-renewal-check`) renews expiring leafs.
//
// Setup: one pool with nodeIdStart=0 (broker id 0) + pre-seeded keystore
// containing a 5-day-valid leaf cert (within the default 30-day renewal
// window). After reconcile, the keystore PATCH body must carry the same
// cert bytes that were seeded.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reconciler_does_not_renew_valid_leaf_certs() {
    let ns = "ns7";
    let name = "c7";
    let pool_name = "brokers";
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let clients_ca_key = format!("{name}-clients-ca");
    let clients_ca_cert = format!("{name}-clients-ca-cert");
    let keystore_name = format!("{name}-kafka-brokers");
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");

    // Generate real CA material so the operator can parse notAfter for status.
    let cluster_ca_mat =
        crabka_security::ca::generate_cluster_ca("c7-cluster-ca", 365).expect("cluster CA gen");
    let clients_ca_mat =
        crabka_security::ca::generate_clients_ca("c7-clients-ca", 365).expect("clients CA gen");

    // Issue a leaf cert for broker 0 with only 5 days validity — inside the
    // 30-day renewal window. The reconciler must NOT replace it.
    let leaf = crabka_security::ca::issue_broker_cert(
        &cluster_ca_mat.cert_pem,
        &cluster_ca_mat.key_pem,
        "c7-brokers-0",
        &[crabka_security::ca::SubjectAltName::Dns(format!(
            "c7-brokers-0.c7-broker-headless.{ns}.svc.cluster.local"
        ))],
        &[],
        5,
    )
    .expect("leaf cert gen");
    let crt_b64 = base64::engine::general_purpose::STANDARD.encode(leaf.cert_pem.as_bytes());
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(leaf.key_pem.as_bytes());

    // Compute the SAN digest that the reconciler will derive for broker 0.
    // SANs match what kafka.rs builds: pod_fqdn, pod_name, headless-svc FQDN, localhost.
    let broker_sans = vec![
        crabka_security::ca::SubjectAltName::Dns(format!(
            "c7-brokers-0.c7-broker-headless.{ns}.svc.cluster.local"
        )),
        crabka_security::ca::SubjectAltName::Dns("c7-brokers-0".into()),
        crabka_security::ca::SubjectAltName::Dns(format!(
            "c7-broker-headless.{ns}.svc.cluster.local"
        )),
        crabka_security::ca::SubjectAltName::Ip(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )),
    ];
    let digest = compute_san_digest(&broker_sans, &[]);
    let digest_b64 = base64::engine::general_purpose::STANDARD.encode(digest.as_bytes());

    // Pre-seeded keystore Secret with broker 0's cert and digest present.
    // The digest matches what the reconciler will compute, so the cert is reused.
    let pre_seeded_keystore = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": &keystore_name, "namespace": ns, "uid": "ks-uid" },
        "type": "Opaque",
        "data": { "0.crt": crt_b64, "0.key": key_b64, "0.sans-digest": digest_b64 }
    });

    // One pool with nodeIdStart=0 (broker id 0) so the reconciler's
    // BrokerCertRequest list has exactly broker_id=0 — matching the
    // pre-seeded keystore entry.
    let pool_item = json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaNodePool",
        "metadata": {
            "name": pool_name, "namespace": ns,
            "uid": "pool-uid", "labels": { "crabka.io/cluster": name }
        },
        "spec": { "roles": ["Controller", "Broker"], "replicas": 1, "nodeIdStart": 0 },
        "status": { "conditions": [], "replicas": 1, "readyReplicas": 1 }
    });
    let pool_body_resp = json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaNodePool",
        "metadata": {
            "name": pool_name, "namespace": ns,
            "uid": "pool-uid", "labels": { "crabka.io/cluster": name }
        },
        "spec": { "roles": ["Controller", "Broker"], "replicas": 1, "nodeIdStart": 0 },
        "status": { "conditions": [] }
    });

    let rules = vec![
        // 1. PATCH headless service
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, ns)),
        },
        // 2. GET cluster-id → 404
        secret_rule_404(Method::GET, format!("/secrets/{secret_name}")),
        // 3. POST cluster-id → 201
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(201, &fake_secret_body_cluster_id(&secret_name, ns)),
        },
        // 4. GET cluster-ca key → 200 (pre-existing CA)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_key, ns, &cluster_ca_mat.key_pem),
            ),
        },
        // 5. GET cluster-ca cert → 200
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_cert, ns, &cluster_ca_mat.cert_pem),
            ),
        },
        // 6. GET clients-ca key → 200
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_key, ns, &clients_ca_mat.key_pem),
            ),
        },
        // 7. GET clients-ca cert → 200
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_cert, ns, &clients_ca_mat.cert_pem),
            ),
        },
        // 8. GET kafkanodepools list → 1 pool (broker 0)
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(&[pool_item])),
        },
        // 9. GET keystore → 200 with pre-seeded broker 0 cert
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &pre_seeded_keystore),
        },
        // 10. PATCH keystore — reconciler always applies via SSA (labels +
        //     ownerRefs); broker 0's entry was found so it is reused verbatim.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &pre_seeded_keystore.clone()),
        },
        // 11. PATCH configmap
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, ns)),
        },
        // 12. PATCH pool owner-ref adopt
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}?"),
            response: json_response(200, &pool_body_resp),
        },
        // 13. PATCH kafka status
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkas/{name}/status"),
            response: json_response(200, &fake_kafka_body(name, ns)),
        },
    ];

    let (ctx, state) = build_ctx(ns, rules);
    let kafka = kafka_cr(name, ns);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let methods_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    // Find the keystore PATCH and verify the 0.crt value is byte-identical
    // to the pre-seeded cert (i.e. the reconciler did not reissue).
    let ks_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains(&keystore_name))
        .expect("keystore PATCH must be present");

    let patch_body: serde_json::Value =
        serde_json::from_slice(ks_patch.body()).expect("keystore PATCH body is JSON");

    let patched_crt = patch_body
        .pointer("/data/0.crt")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            panic!("keystore PATCH body must contain data['0.crt'], body = {patch_body}")
        });

    let original_bytes = leaf.cert_pem.as_bytes();
    let patched_bytes = base64::engine::general_purpose::STANDARD
        .decode(patched_crt)
        .expect("0.crt is base64");
    assert!(
        patched_bytes == original_bytes,
        "reconciler must not replace an existing leaf cert; cert bytes must be identical"
    );

    // Confirm no CA PATCHes happened (CAs were reused from existing Secrets).
    let ca_patches: Vec<_> = methods_uris
        .iter()
        .filter(|(m, u)| {
            *m == Method::PATCH && (u.contains("-cluster-ca") || u.contains("-clients-ca"))
        })
        .collect();
    assert!(
        ca_patches.is_empty(),
        "reconciler must not PATCH CA Secrets when they already exist: {ca_patches:?}",
    );

    assert!(
        state.remaining_rules() == 0,
        "all preloaded rules must have been consumed"
    );
}

// ---------------------------------------------------------------------------
// Shared helpers for tests 2, 3, 4
// ---------------------------------------------------------------------------

fn pool_item(cluster: &str, ns: &str, pool_name: &str, node_id: i32) -> serde_json::Value {
    json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaNodePool",
        "metadata": {
            "name": pool_name, "namespace": ns,
            "uid": format!("{pool_name}-uid"),
            "labels": { "crabka.io/cluster": cluster }
        },
        "spec": { "roles": ["Controller", "Broker"], "replicas": 1, "nodeIdStart": node_id },
        "status": { "conditions": [], "replicas": 1, "readyReplicas": 1 }
    })
}

fn pool_body_resp(cluster: &str, ns: &str, pool_name: &str, node_id: i32) -> serde_json::Value {
    json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaNodePool",
        "metadata": {
            "name": pool_name, "namespace": ns,
            "uid": format!("{pool_name}-uid"),
            "labels": { "crabka.io/cluster": cluster }
        },
        "spec": { "roles": ["Controller", "Broker"], "replicas": 1, "nodeIdStart": node_id },
        "status": { "conditions": [] }
    })
}

fn broker_sans(
    cluster: &str,
    pool_name: &str,
    ns: &str,
) -> Vec<crabka_security::ca::SubjectAltName> {
    vec![
        crabka_security::ca::SubjectAltName::Dns(format!(
            "{cluster}-{pool_name}-0.{cluster}-broker-headless.{ns}.svc.cluster.local"
        )),
        crabka_security::ca::SubjectAltName::Dns(format!("{cluster}-{pool_name}-0")),
        crabka_security::ca::SubjectAltName::Dns(format!(
            "{cluster}-broker-headless.{ns}.svc.cluster.local"
        )),
        crabka_security::ca::SubjectAltName::Ip(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )),
    ]
}

/// Locate the keystore PATCH in the observed request log and return its
/// `data` map (raw base64-encoded values). Panics if no PATCH was issued.
fn keystore_patch_data<B: AsRef<[u8]>>(
    observed: &[http::Request<B>],
    keystore_name: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains(keystore_name))
        .expect("keystore PATCH must be present");
    let body: serde_json::Value =
        serde_json::from_slice(patch.body().as_ref()).expect("keystore PATCH body is JSON");
    body.pointer("/data")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_else(|| panic!("keystore PATCH body has no /data object, body = {body}"))
}

// ---------------------------------------------------------------------------
// Test 2: broker leaf certs chain to cluster CA
//
// One pool → one broker leaf. The leaf cert written to the keystore PATCH must:
//   1. Parse as a valid X.509 cert.
//   2. Carry an `issuer` DN equal to the cluster CA's `subject` DN.
//   3. Verify signature against the cluster CA public key (chain-to-root).
//   4. List the expected pod-FQDN / pod-name / headless / 127.0.0.1 SANs.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn broker_leaf_certs_chain_to_cluster_ca() {
    let ns = "ns2";
    let name = "c2";
    let pool_name = "brokers";
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let clients_ca_key = format!("{name}-clients-ca");
    let clients_ca_cert = format!("{name}-clients-ca-cert");
    let keystore_name = format!("{name}-kafka-brokers");
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");

    // BYO so the cluster CA stays stable and we control the PEM directly.
    let cluster_ca_mat =
        crabka_security::ca::generate_cluster_ca("c2-cluster-ca", 365).expect("cluster CA gen");
    let clients_ca_mat =
        crabka_security::ca::generate_clients_ca("c2-clients-ca", 365).expect("clients CA gen");

    let pool_one = pool_item(name, ns, pool_name, 0);
    let pool_resp = pool_body_resp(name, ns, pool_name, 0);

    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, ns)),
        },
        secret_rule_404(Method::GET, format!("/secrets/{secret_name}")),
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(201, &fake_secret_body_cluster_id(&secret_name, ns)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_key, ns, &cluster_ca_mat.key_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_cert, ns, &cluster_ca_mat.cert_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_key, ns, &clients_ca_mat.key_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_cert, ns, &clients_ca_mat.cert_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(&[pool_one])),
        },
        secret_rule_404(Method::GET, format!("/secrets/{keystore_name}")),
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &fake_keystore_secret(&keystore_name, ns)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, ns)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}?"),
            response: json_response(200, &pool_resp),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkas/{name}/status"),
            response: json_response(200, &fake_kafka_body(name, ns)),
        },
    ];

    let (ctx, state) = build_ctx(ns, rules);
    let kafka = kafka_cr_byo(name, ns);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let data = keystore_patch_data(&observed, &keystore_name);

    let crt_b64 = data
        .get("0.crt")
        .and_then(|v| v.as_str())
        .expect("keystore PATCH must contain data['0.crt']");
    let leaf_pem_bytes = base64::engine::general_purpose::STANDARD
        .decode(crt_b64)
        .expect("0.crt is base64");
    let leaf_pem = std::str::from_utf8(&leaf_pem_bytes).expect("leaf PEM is utf-8");

    let (_, leaf_pem_block) = parse_x509_pem(leaf_pem.as_bytes()).expect("leaf parses as PEM");
    let leaf = leaf_pem_block
        .parse_x509()
        .expect("leaf PEM decodes to X509Certificate");

    let (_, ca_pem_block) =
        parse_x509_pem(cluster_ca_mat.cert_pem.as_bytes()).expect("cluster CA parses as PEM");
    let ca = ca_pem_block
        .parse_x509()
        .expect("cluster CA PEM decodes to X509Certificate");

    // (2) Issuer of leaf == subject of cluster CA.
    assert!(
        leaf.issuer() == ca.subject(),
        "leaf issuer DN must match cluster CA subject DN; leaf issuer = {}, ca subject = {}",
        leaf.issuer(),
        ca.subject()
    );

    // (3) Signature chain-to-root.
    leaf.verify_signature(Some(ca.public_key()))
        .expect("leaf cert must verify against cluster CA public key");

    // (4) Expected SAN list (pod_fqdn, pod_name, headless FQDN, 127.0.0.1).
    let san_ext = leaf
        .extensions()
        .iter()
        .find_map(|e| match e.parsed_extension() {
            x509_parser::extensions::ParsedExtension::SubjectAlternativeName(san) => Some(san),
            _ => None,
        })
        .expect("leaf must carry a SubjectAlternativeName extension");
    let dns_names: Vec<String> = san_ext
        .general_names
        .iter()
        .filter_map(|gn| match gn {
            x509_parser::extensions::GeneralName::DNSName(s) => Some((*s).to_owned()),
            _ => None,
        })
        .collect();
    let has_ip_localhost = san_ext.general_names.iter().any(|gn| {
        matches!(gn, x509_parser::extensions::GeneralName::IPAddress(b) if *b == [127, 0, 0, 1])
    });

    let pod_name = format!("{name}-{pool_name}-0");
    let pod_fqdn = format!("{pod_name}.{name}-broker-headless.{ns}.svc.cluster.local");
    let headless_fqdn = format!("{name}-broker-headless.{ns}.svc.cluster.local");
    // Pod FQDN, pod short-name, headless FQDN.
    for want in [&pod_fqdn, &pod_name, &headless_fqdn] {
        assert!(
            dns_names.contains(want),
            "SANs must include {want}; got DNS={dns_names:?}",
        );
    }
    assert!(
        has_ip_localhost,
        "SANs must include 127.0.0.1; got GNs={:?}",
        san_ext.general_names,
    );

    assert!(
        state.remaining_rules() == 0,
        "all preloaded rules must have been consumed"
    );
}

// ---------------------------------------------------------------------------
// Test 3: scale-up adds entries, does not reissue existing
//
// Pre-seed keystore with brokers 0, 1, 2. Configure pool list with 4 pools
// (broker 0..=3). After reconcile, the keystore PATCH must contain:
//   - 0.crt, 1.crt, 2.crt byte-identical to the pre-seeded entries
//     (and matching keys / digests likewise);
//   - new 3.crt, 3.key, 3.sans-digest entries for the added broker.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scale_up_adds_entries_does_not_reissue_existing() {
    let ns = "ns3";
    let name = "c3";
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let clients_ca_key = format!("{name}-clients-ca");
    let clients_ca_cert = format!("{name}-clients-ca-cert");
    let keystore_name = format!("{name}-kafka-brokers");
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");

    let cluster_ca_mat =
        crabka_security::ca::generate_cluster_ca("c3-cluster-ca", 365).expect("cluster CA gen");
    let clients_ca_mat =
        crabka_security::ca::generate_clients_ca("c3-clients-ca", 365).expect("clients CA gen");

    // Pre-issue leaf certs for brokers 0, 1, 2 against the BYO cluster CA with
    // the SAN list the reconciler will compute. Matching SAN digests trigger
    // the reuse path in `ensure_broker_keystore`.
    let mut pre_data = serde_json::Map::new();
    let pool_names = ["pool-a", "pool-b", "pool-c"];
    let mut original_certs: std::collections::BTreeMap<i32, Vec<u8>> =
        std::collections::BTreeMap::new();
    for (i, pool) in pool_names.iter().enumerate() {
        let id = i32::try_from(i).unwrap();
        let sans = broker_sans(name, pool, ns);
        let leaf = crabka_security::ca::issue_broker_cert(
            &cluster_ca_mat.cert_pem,
            &cluster_ca_mat.key_pem,
            &format!("{name}-{pool}-0"),
            &sans,
            &[],
            365,
        )
        .expect("leaf cert gen");
        let digest = compute_san_digest(&sans, &[]);
        let crt_b64 = base64::engine::general_purpose::STANDARD.encode(leaf.cert_pem.as_bytes());
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(leaf.key_pem.as_bytes());
        let dig_b64 = base64::engine::general_purpose::STANDARD.encode(digest.as_bytes());
        pre_data.insert(format!("{id}.crt"), json!(crt_b64));
        pre_data.insert(format!("{id}.key"), json!(key_b64));
        pre_data.insert(format!("{id}.sans-digest"), json!(dig_b64));
        original_certs.insert(id, leaf.cert_pem.into_bytes());
    }
    let pre_seeded_keystore = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": &keystore_name, "namespace": ns, "uid": "ks-uid" },
        "type": "Opaque",
        "data": pre_data
    });

    // 4 pools: brokers 0, 1, 2 already provisioned + new broker 3.
    let new_pool = "pool-d";
    let pool_items: Vec<serde_json::Value> = (0_usize..4)
        .map(|i| {
            let pname = if i < pool_names.len() {
                pool_names[i]
            } else {
                new_pool
            };
            pool_item(name, ns, pname, i32::try_from(i).unwrap())
        })
        .collect();

    let mut rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, ns)),
        },
        secret_rule_404(Method::GET, format!("/secrets/{secret_name}")),
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(201, &fake_secret_body_cluster_id(&secret_name, ns)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_key, ns, &cluster_ca_mat.key_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_cert, ns, &cluster_ca_mat.cert_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_key, ns, &clients_ca_mat.key_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_cert, ns, &clients_ca_mat.cert_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(&pool_items)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &pre_seeded_keystore),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &pre_seeded_keystore),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, ns)),
        },
    ];
    // One pool owner-ref-adopt PATCH per pool.
    for (id, pname) in [0, 1, 2, 3].iter().zip(
        pool_names
            .iter()
            .map(|s| (*s).to_string())
            .chain(std::iter::once(new_pool.to_string())),
    ) {
        rules.push(MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pname}?"),
            response: json_response(200, &pool_body_resp(name, ns, &pname, *id)),
        });
    }
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkas/{name}/status"),
        response: json_response(200, &fake_kafka_body(name, ns)),
    });

    let (ctx, state) = build_ctx(ns, rules);
    let kafka = kafka_cr_byo(name, ns);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let data = keystore_patch_data(&observed, &keystore_name);

    // Existing brokers 0, 1, 2: cert bytes byte-identical to pre-seed.
    for id in 0..3i32 {
        let crt_b64 = data
            .get(&format!("{id}.crt"))
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("keystore PATCH missing data['{id}.crt']"));
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(crt_b64)
            .expect("base64");
        assert!(
            bytes == original_certs[&id],
            "broker {id} cert must be byte-identical (reuse path), not reissued"
        );
    }

    // New broker 3: cert + key + digest present.
    for k in ["3.crt", "3.key", "3.sans-digest"] {
        assert!(
            data.get(k).and_then(|v| v.as_str()).is_some(),
            "scale-up: keystore PATCH must add data['{k}'], got keys = {:?}",
            data.keys().collect::<Vec<_>>(),
        );
    }
    // New broker 3's cert must also chain to the cluster CA (sanity).
    let new_crt_b64 = data["3.crt"].as_str().unwrap();
    let new_pem_bytes = base64::engine::general_purpose::STANDARD
        .decode(new_crt_b64)
        .unwrap();
    let new_pem = std::str::from_utf8(&new_pem_bytes).expect("utf-8 PEM");
    let (_, leaf_block) = parse_x509_pem(new_pem.as_bytes()).expect("new leaf parses as PEM");
    let leaf = leaf_block.parse_x509().expect("new leaf decodes to X509");
    let (_, ca_block) =
        parse_x509_pem(cluster_ca_mat.cert_pem.as_bytes()).expect("cluster CA parses as PEM");
    let ca = ca_block.parse_x509().expect("CA decodes to X509");
    leaf.verify_signature(Some(ca.public_key()))
        .expect("new leaf must chain to cluster CA");

    assert!(
        state.remaining_rules() == 0,
        "all preloaded rules must have been consumed"
    );
}

// ---------------------------------------------------------------------------
// Test 4: scale-down prunes entries
//
// Pre-seed keystore with brokers 0, 1, 2, 3. Configure pool list with only 3
// pools (broker 0..=2). After reconcile, the keystore PATCH must:
//   - keep 0.crt, 1.crt, 2.crt byte-identical (reuse path), and
//   - drop 3.crt, 3.key, 3.sans-digest entirely (prune path).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scale_down_prunes_entries() {
    let ns = "ns4";
    let name = "c4";
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let clients_ca_key = format!("{name}-clients-ca");
    let clients_ca_cert = format!("{name}-clients-ca-cert");
    let keystore_name = format!("{name}-kafka-brokers");
    let svc_name = format!("{name}-broker-headless");
    let cm_name = format!("{name}-broker-config");
    let secret_name = format!("{name}-cluster-id");

    let cluster_ca_mat =
        crabka_security::ca::generate_cluster_ca("c4-cluster-ca", 365).expect("cluster CA gen");
    let clients_ca_mat =
        crabka_security::ca::generate_clients_ca("c4-clients-ca", 365).expect("clients CA gen");

    // Pre-issue 4 brokers (0..=3). The "removed" broker (id 3) uses a pool name
    // that won't be in the post-scale-down pool list.
    let kept_pool_names = ["pool-a", "pool-b", "pool-c"];
    let removed_pool_name = "pool-d";
    let mut pre_data = serde_json::Map::new();
    let mut original_certs: std::collections::BTreeMap<i32, Vec<u8>> =
        std::collections::BTreeMap::new();
    for i in 0_usize..4 {
        let id = i32::try_from(i).unwrap();
        let pname = if i < kept_pool_names.len() {
            kept_pool_names[i]
        } else {
            removed_pool_name
        };
        let sans = broker_sans(name, pname, ns);
        let leaf = crabka_security::ca::issue_broker_cert(
            &cluster_ca_mat.cert_pem,
            &cluster_ca_mat.key_pem,
            &format!("{name}-{pname}-0"),
            &sans,
            &[],
            365,
        )
        .expect("leaf cert gen");
        let digest = compute_san_digest(&sans, &[]);
        let crt_b64 = base64::engine::general_purpose::STANDARD.encode(leaf.cert_pem.as_bytes());
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(leaf.key_pem.as_bytes());
        let dig_b64 = base64::engine::general_purpose::STANDARD.encode(digest.as_bytes());
        pre_data.insert(format!("{id}.crt"), json!(crt_b64));
        pre_data.insert(format!("{id}.key"), json!(key_b64));
        pre_data.insert(format!("{id}.sans-digest"), json!(dig_b64));
        original_certs.insert(id, leaf.cert_pem.into_bytes());
    }
    let pre_seeded_keystore = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": &keystore_name, "namespace": ns, "uid": "ks-uid" },
        "type": "Opaque",
        "data": pre_data
    });

    // Pool list now contains only 3 pools (0..=2). Broker 3's pool is gone.
    let pool_items: Vec<serde_json::Value> = (0_usize..3)
        .map(|i| pool_item(name, ns, kept_pool_names[i], i32::try_from(i).unwrap()))
        .collect();

    let mut rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, ns)),
        },
        secret_rule_404(Method::GET, format!("/secrets/{secret_name}")),
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(201, &fake_secret_body_cluster_id(&secret_name, ns)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_key, ns, &cluster_ca_mat.key_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&cluster_ca_cert, ns, &cluster_ca_mat.cert_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_key, ns, &clients_ca_mat.key_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(
                200,
                &fake_ca_secret_with_pem(&clients_ca_cert, ns, &clients_ca_mat.cert_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(&pool_items)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &pre_seeded_keystore),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &pre_seeded_keystore),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/configmaps/{cm_name}"),
            response: json_response(200, &fake_configmap_body(&cm_name, ns)),
        },
    ];
    for (id, pname) in [0i32, 1, 2].iter().zip(kept_pool_names.iter()) {
        rules.push(MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pname}?"),
            response: json_response(200, &pool_body_resp(name, ns, pname, *id)),
        });
    }
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkas/{name}/status"),
        response: json_response(200, &fake_kafka_body(name, ns)),
    });

    let (ctx, state) = build_ctx(ns, rules);
    let kafka = kafka_cr_byo(name, ns);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let data = keystore_patch_data(&observed, &keystore_name);

    // Remaining brokers 0, 1, 2: byte-identical to pre-seed (reuse path).
    for id in 0..3i32 {
        let crt_b64 = data
            .get(&format!("{id}.crt"))
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("keystore PATCH missing data['{id}.crt']"));
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(crt_b64)
            .expect("base64");
        assert!(
            bytes == original_certs[&id],
            "broker {id} cert must be byte-identical after scale-down (reuse path)"
        );
    }

    // Removed broker 3: all three entries pruned.
    for k in ["3.crt", "3.key", "3.sans-digest"] {
        assert!(
            !data.contains_key(k),
            "scale-down: keystore PATCH must drop data['{k}'], got keys = {:?}",
            data.keys().collect::<Vec<_>>(),
        );
    }

    assert!(
        state.remaining_rules() == 0,
        "all preloaded rules must have been consumed"
    );
}
