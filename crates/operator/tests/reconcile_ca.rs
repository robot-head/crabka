//! Slice 30: integration tests for CA + broker-keystore reconciliation.
//!
//! Test list (Task 10):
//!   1. `default_flow_creates_cluster_ca_clients_ca_and_broker_keystore`
//!   2. `broker_leaf_certs_chain_to_cluster_ca`  (TODO: slice-30 follow-up)
//!   3. `scale_up_adds_entries_does_not_reissue_existing`  (TODO: slice-30 follow-up)
//!   4. `scale_down_prunes_entries`  (TODO: slice-30 follow-up)
//!   5. `byo_mode_adopts_pre_existing_secrets_does_not_overwrite`
//!   6. `byo_mode_without_pre_existing_secrets_errors_gracefully`
//!   7. `reconciler_does_not_renew_valid_leaf_certs`

#[path = "shared/mod.rs"]
mod shared;

use std::sync::Arc;

use base64::Engine as _;
use crabka_operator::controller::kafka::reconcile;
use crabka_operator::crd::{CertificateAuthority, Kafka, KafkaSpec};
use http::{Method, Response};
use serde_json::json;

use crabka_operator::controller::cluster_ca::compute_san_digest;
use shared::{
    MockRule, build_ctx, fake_ca_secret, fake_configmap_body, fake_kafka_body,
    fake_keystore_secret, fake_pool_list_body, fake_service_body, json_response, not_found_body,
};

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
#[allow(clippy::too_many_lines)]
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
    assert_eq!(
        ca_patches.len(),
        5,
        "expected 5 CA-related PATCH calls (2 cluster-ca, 2 clients-ca, 1 keystore), \
         got {}: {:?}",
        ca_patches.len(),
        ca_patches,
    );

    // cluster-ca key + cert PATCHes present
    assert!(
        methods_uris
            .iter()
            .any(|(m, u)| *m == Method::PATCH && u.contains(&cluster_ca_key)),
        "cluster-ca key PATCH must be present",
    );
    assert!(
        methods_uris
            .iter()
            .any(|(m, u)| *m == Method::PATCH && u.contains(&cluster_ca_cert)),
        "cluster-ca cert PATCH must be present",
    );
    // clients-ca key + cert PATCHes present
    assert!(
        methods_uris
            .iter()
            .any(|(m, u)| *m == Method::PATCH && u.contains(&clients_ca_key)),
        "clients-ca key PATCH must be present",
    );
    assert!(
        methods_uris
            .iter()
            .any(|(m, u)| *m == Method::PATCH && u.contains(&clients_ca_cert)),
        "clients-ca cert PATCH must be present",
    );
    // keystore PATCH present
    assert!(
        methods_uris
            .iter()
            .any(|(m, u)| *m == Method::PATCH && u.contains(&keystore_name)),
        "broker keystore PATCH must be present",
    );

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
    assert_eq!(cluster_ca_cond["status"], "True", "body = {body}");
    assert_eq!(cluster_ca_cond["reason"], "CaReady", "body = {body}");
    let clients_ca_cond = conds
        .iter()
        .find(|c| c["type"] == "ClientsCaReady")
        .unwrap_or_else(|| panic!("ClientsCaReady condition missing, body = {body}"));
    assert_eq!(clients_ca_cond["status"], "True", "body = {body}");
    assert_eq!(clients_ca_cond["reason"], "CaReady", "body = {body}");

    assert_eq!(
        state.remaining_rules(),
        0,
        "all preloaded rules must have been consumed"
    );
}

// ---------------------------------------------------------------------------
// Tests 2, 3, 4 — TODO: slice-30 follow-up
//
// Test 2 (broker leaf certs chain to cluster CA): requires x509-parsing the
//   leaf cert from the keystore PATCH body and verifying the signature chain.
//   Deferred because the FIFO mock returns a hand-crafted empty Secret body on
//   the keystore PATCH; the operator writes the real certs but the mock response
//   does not echo them back. The test would need to intercept the PATCH *request*
//   body (not the response) and parse certs from it — possible but not yet plumbed.
//
// Tests 3 & 4 (scale-up / scale-down): require a two-reconcile sequence where
//   the second GET for the keystore returns what was PATCHed in the first
//   reconcile. This needs either an Arc<Mutex<Option<Secret>>> in the mock state
//   or hand-crafting a pre-seeded keystore body for the second reconcile call.
//   Deferred to a follow-up task.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Test 5: BYO mode adopts pre-existing Secrets, doesn't overwrite them
//
// The operator must NOT PATCH the CA Secrets when they already exist.
// It must still PATCH the broker keystore (signed against the BYO CA).
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(clippy::too_many_lines)]
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
    assert_eq!(
        cluster_ca_cond["status"], "True",
        "BYO present: ClusterCaReady must be True, body = {body}"
    );
    let clients_ca_cond = conds
        .iter()
        .find(|c| c["type"] == "ClientsCaReady")
        .unwrap_or_else(|| panic!("ClientsCaReady condition missing, body = {body}"));
    assert_eq!(
        clients_ca_cond["status"], "True",
        "BYO present: ClientsCaReady must be True, body = {body}"
    );

    assert_eq!(
        state.remaining_rules(),
        0,
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
        // 3b. Slice 34: pools are listed before the CA reconcile (rollout
        // convergence check), so the BYO-missing early-out now sees a pool LIST.
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
    assert_eq!(
        cluster_ca_cond["status"], "False",
        "ByoCaMissing: ClusterCaReady must be False, body = {body}"
    );
    assert_eq!(
        cluster_ca_cond["reason"], "ByoCaMissing",
        "ByoCaMissing: reason must be ByoCaMissing, body = {body}"
    );

    assert_eq!(
        state.remaining_rules(),
        0,
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
#[allow(clippy::too_many_lines)]
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
    assert_eq!(
        patched_bytes, original_bytes,
        "reconciler must not replace an existing leaf cert; cert bytes must be identical",
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

    assert_eq!(
        state.remaining_rules(),
        0,
        "all preloaded rules must have been consumed"
    );
}
