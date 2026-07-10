//! Integration tests for the `ca-renewal-check` `CronJob` entry
//! (`crabka_operator::controller::cluster_ca::run_renewal_check`).
//!
//! Each test seeds the mock client with canned CA + broker-keystore Secrets
//! (using real PEM material from `crabka_security::ca`), calls
//! `run_renewal_check`, and asserts on the observed request log.

use assert2::{assert, check};
#[path = "shared/mod.rs"]
mod shared;

use base64::Engine as _;
use crabka_operator::controller::cluster_ca::run_renewal_check;
use crabka_security::ca::{
    SubjectAltName, generate_clients_ca, generate_cluster_ca, issue_broker_cert,
};
use http::{Method, Response};
use shared::{MockRule, MockState, json_response, mock_client, not_found_body};

// ---------------------------------------------------------------------------
// Helper: build a Secret JSON body with arbitrary `data` fields (base64
// encoded). We re-encode PEM strings into base64 here because Kubernetes
// `Secret.data` values are always base64.
// ---------------------------------------------------------------------------

fn pem_secret(name: &str, namespace: &str, entries: &[(&str, &str)]) -> serde_json::Value {
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut data = serde_json::Map::new();
    for (key, pem) in entries {
        data.insert(
            key.to_string(),
            serde_json::Value::String(b64.encode(pem.as_bytes())),
        );
    }
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "secret-uid" },
        "type": "Opaque",
        "data": data,
    })
}

/// Build a `KafkaList` body that contains a single Kafka object.
fn kafka_list_body(name: &str, namespace: &str, generate_ca: bool) -> serde_json::Value {
    let mut cluster_ca_spec = serde_json::json!({
        "generateCertificateAuthority": generate_ca,
        "validityDays": 365,
        "renewalDays": 30,
    });
    // For BYO we set the same spec, just generateCertificateAuthority=false
    if !generate_ca {
        cluster_ca_spec["generateCertificateAuthority"] = serde_json::Value::Bool(false);
    }
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaList",
        "metadata": { "resourceVersion": "1" },
        "items": [{
            "apiVersion": "crabka.io/v1alpha1",
            "kind": "Kafka",
            "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
            "spec": {
                "kafkaVersion": "0.1.1",
                "clusterCa": cluster_ca_spec.clone(),
                "clientsCa": cluster_ca_spec,
            },
            "status": { "conditions": [] }
        }],
    })
}

/// Build a minimal Kafka status GET response (for the `get_status` call
/// inside `flag_ca_if_expiring`).
fn kafka_status_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": { "kafkaVersion": "0.1.1" },
        "status": { "conditions": [] }
    })
}

/// Faked Event creation response — kube-rs POSTs Events and needs a body
/// that deserializes back into an `Event`.
fn fake_event_body(namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": { "name": "crabka-ca-renewal-abc", "namespace": namespace, "uid": "event-uid" },
        "involvedObject": {},
        "message": "test event",
        "reason": "TestReason",
        "type": "Normal",
        "reportingComponent": "crabka-operator/ca-renewal-check",
        "reportingInstance": "crabka-operator-renewal",
        "eventTime": null,
    })
}

// ---------------------------------------------------------------------------
// Test 1: broker leaf certs within renewal window → reissued
// ---------------------------------------------------------------------------

/// Seed a broker keystore Secret with a leaf cert whose `notAfter` is
/// 5 days out (< 30-day renewal window). `run_renewal_check` must:
///   1. PATCH the broker-keystore Secret with a new `0.crt` (i.e. make
///      exactly one PATCH to the `-kafka-brokers` Secret).
///   2. POST a `Normal` Event with reason `BrokerCertRenewed`.
///
/// The cluster CA and clients CA are intentionally fresh (365-day validity)
/// so the CA-expiry path is NOT triggered.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cronjob_reissues_aging_broker_leafs() {
    let ns = "default";
    let cluster = "c1";

    // --- Build real PEM material ---
    // CA and clients-CA are fresh (365 days), so flag_ca_if_expiring is a no-op.
    let ca = generate_cluster_ca("c1-cluster-ca", 365).expect("cluster CA");
    let clients_ca = generate_clients_ca("c1-clients-ca", 365).expect("clients CA");

    // Broker leaf cert is almost expired (5-day validity).
    let sans = vec![SubjectAltName::Dns("c1-brokers-0".into())];
    let old_leaf = issue_broker_cert(
        &ca.cert_pem,
        &ca.key_pem,
        "c1-brokers-0",
        &sans,
        &[],
        5, // 5 days → within 30-day renewal window
    )
    .expect("broker leaf");

    // Keystore secret containing the old leaf cert at key "0.crt".
    let keystore_body = {
        let b64 = base64::engine::general_purpose::STANDARD;
        let mut data = serde_json::Map::new();
        data.insert(
            "0.crt".into(),
            serde_json::Value::String(b64.encode(old_leaf.cert_pem.as_bytes())),
        );
        data.insert(
            "0.key".into(),
            serde_json::Value::String(b64.encode(old_leaf.key_pem.as_bytes())),
        );
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": format!("{cluster}-kafka-brokers"),
                "namespace": ns,
                "uid": "ks-uid",
                "resourceVersion": "42",
            },
            "type": "Opaque",
            "data": data,
        })
    };

    let rules = vec![
        // 1. LIST kafkas in namespace → one Kafka c1
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkas"),
            response: json_response(200, &kafka_list_body(cluster, ns, true)),
        },
        // 2. GET cluster-ca key secret
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-cluster-ca"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-cluster-ca"),
                    ns,
                    &[("ca.key", &ca.key_pem)],
                ),
            ),
        },
        // 3. GET cluster-ca-cert secret
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-cluster-ca-cert"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-cluster-ca-cert"),
                    ns,
                    &[("ca.crt", &ca.cert_pem)],
                ),
            ),
        },
        // 4. GET clients-ca key secret
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-clients-ca"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-clients-ca"),
                    ns,
                    &[("ca.key", &clients_ca.key_pem)],
                ),
            ),
        },
        // 5. GET clients-ca-cert secret
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-clients-ca-cert"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-clients-ca-cert"),
                    ns,
                    &[("ca.crt", &clients_ca.cert_pem)],
                ),
            ),
        },
        // 6. Neither CA is expiring → no event/status PATCHes for CA.

        // 7. GET broker keystore → present with expiring leaf
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-kafka-brokers"),
            response: json_response(200, &keystore_body),
        },
        // 8. PATCH broker keystore (renewed cert)
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster}-kafka-brokers"),
            response: json_response(200, &keystore_body),
        },
        // 9. POST event BrokerCertRenewed
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/events"),
            response: json_response(201, &fake_event_body(ns)),
        },
    ];

    let state = MockState::new(rules);
    let client = mock_client(&state, ns);

    run_renewal_check(client, Some(ns))
        .await
        .expect("renewal check succeeds");

    let observed = state.take_observed();
    let methods_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    // Must have patched the keystore.
    let keystore_patch = observed.iter().find(|r| {
        r.method() == Method::PATCH
            && r.uri()
                .to_string()
                .contains(&format!("/secrets/{cluster}-kafka-brokers"))
    });
    assert!(
        keystore_patch.is_some(),
        "expected PATCH to kafka-brokers keystore; requests: {methods_uris:?}",
    );

    // PATCH body must contain "0.crt" key and it must differ from the seeded cert.
    let patch_body: serde_json::Value =
        serde_json::from_slice(keystore_patch.unwrap().body()).expect("patch body is JSON");
    let new_crt_b64 = patch_body["data"]["0.crt"]
        .as_str()
        .expect("data[0.crt] present in PATCH body");
    let b64 = base64::engine::general_purpose::STANDARD;
    let old_crt_b64 = b64.encode(old_leaf.cert_pem.as_bytes());
    assert!(
        new_crt_b64 != old_crt_b64.as_str(),
        "renewed cert must differ from the pre-seeded cert"
    );

    // Must have emitted a BrokerCertRenewed event.
    let event_post = observed.iter().find(|r| {
        r.method() == Method::POST
            && r.uri()
                .to_string()
                .contains(&format!("/namespaces/{ns}/events"))
    });
    assert!(
        event_post.is_some(),
        "expected POST to /events; requests: {methods_uris:?}",
    );
    let event_body: serde_json::Value =
        serde_json::from_slice(event_post.unwrap().body()).expect("event body is JSON");
    assert_eq!(
        (event_body["reason"].as_str(), event_body["type"].as_str()),
        (Some("BrokerCertRenewed"), Some("Normal")),
        "body = {event_body}"
    );

    check!(state.remaining_rules() == 0, "all rules consumed");
}

// ---------------------------------------------------------------------------
// Test 2: expiring cluster CA → Warning event + status PATCH, no CA rotation
// ---------------------------------------------------------------------------

/// Seed a cluster CA with `notAfter = now + 25 days` (< 30-day renewal
/// window). The `CronJob` does not flag rotation disruptively — it
/// nudges the reconciler. `run_renewal_check` must:
///   1. NOT PATCH the cluster-ca Secret (the reconciler owns rotation).
///   2. PATCH the Kafka CR metadata stamping `crabka.io/ca-renew-after`.
///   3. POST a `Normal` Event with reason `CaRenewalScheduled`.
///
/// Clients CA and broker keystore are fresh / absent so they don't trigger
/// additional paths.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cronjob_flags_expiring_cluster_ca_without_rotating() {
    let ns = "default";
    let cluster = "c1";

    // Cluster CA is almost expired (25 days → within 30-day renewal window).
    let expiring_ca = generate_cluster_ca("c1-cluster-ca", 25).expect("expiring cluster CA");
    // Clients CA is fresh.
    let clients_ca = generate_clients_ca("c1-clients-ca", 365).expect("clients CA");

    let rules = vec![
        // 1. LIST kafkas
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkas"),
            response: json_response(200, &kafka_list_body(cluster, ns, true)),
        },
        // 2. GET cluster-ca key
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-cluster-ca"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-cluster-ca"),
                    ns,
                    &[("ca.key", &expiring_ca.key_pem)],
                ),
            ),
        },
        // 3. GET cluster-ca-cert
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-cluster-ca-cert"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-cluster-ca-cert"),
                    ns,
                    &[("ca.crt", &expiring_ca.cert_pem)],
                ),
            ),
        },
        // 4. GET clients-ca key
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-clients-ca"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-clients-ca"),
                    ns,
                    &[("ca.key", &clients_ca.key_pem)],
                ),
            ),
        },
        // 5. GET clients-ca-cert
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-clients-ca-cert"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-clients-ca-cert"),
                    ns,
                    &[("ca.crt", &clients_ca.cert_pem)],
                ),
            ),
        },
        // 6. flag_ca_if_expiring: stamp the ca-renew-after annotation
        //    on the Kafka CR (nudges the reconciler to run a same-key renewal).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkas/{cluster}"),
            response: json_response(200, &kafka_status_body(cluster, ns)),
        },
        // 7. POST a Normal CaRenewalScheduled event.
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/events"),
            response: json_response(201, &fake_event_body(ns)),
        },
        // 8. clients CA not expiring → no event.
        // 9. GET broker keystore → absent (early return from renew_broker_leafs)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-kafka-brokers"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
    ];

    let state = MockState::new(rules);
    let client = mock_client(&state, ns);

    run_renewal_check(client, Some(ns))
        .await
        .expect("renewal check succeeds");

    let observed = state.take_observed();
    let methods_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    // Must NOT have patched any CA Secret (no rotation of the CA itself).
    let ca_secret_patch = observed.iter().find(|r| {
        r.method() == Method::PATCH
            && (r
                .uri()
                .to_string()
                .contains(&format!("{cluster}-cluster-ca"))
                && !r.uri().to_string().contains("/status"))
    });
    assert!(
        ca_secret_patch.is_none(),
        "cluster-ca Secret must NOT be patched (no rotation); requests: {methods_uris:?}",
    );

    // Must have emitted a Normal CaRenewalScheduled event.
    let event_post = observed.iter().find(|r| {
        r.method() == Method::POST
            && r.uri()
                .to_string()
                .contains(&format!("/namespaces/{ns}/events"))
    });
    assert!(
        event_post.is_some(),
        "expected POST to /events; requests: {methods_uris:?}",
    );
    let event_body: serde_json::Value =
        serde_json::from_slice(event_post.unwrap().body()).expect("event body is JSON");
    assert_eq!(
        (event_body["reason"].as_str(), event_body["type"].as_str()),
        (Some("CaRenewalScheduled"), Some("Normal")),
        "body = {event_body}"
    );

    // Must have PATCHed the Kafka CR metadata with the ca-renew-after annotation
    // (the nudge), NOT the status with CaRotationRequired.
    let meta_patch = observed.iter().find(|r| {
        r.method() == Method::PATCH
            && r.uri().to_string().contains(&format!("/kafkas/{cluster}"))
            && !r.uri().to_string().contains("/status")
    });
    assert!(
        meta_patch.is_some(),
        "expected metadata PATCH to /kafkas/{cluster}; requests: {methods_uris:?}",
    );
    let meta_body: serde_json::Value =
        serde_json::from_slice(meta_patch.unwrap().body()).expect("metadata PATCH body is JSON");
    assert!(
        meta_body["metadata"]["annotations"]["crabka.io/ca-renew-after"].is_string(),
        "metadata PATCH must stamp crabka.io/ca-renew-after; body = {meta_body}",
    );

    assert!(state.remaining_rules() == 0, "all rules consumed");
}

// ---------------------------------------------------------------------------
// Test 3: BYO CA expiring → ByoCaExpiringSoon Warning, no CA PATCH, no status
// ---------------------------------------------------------------------------

/// Seed a BYO CA (`generateCertificateAuthority: false`) with an expiring
/// `notAfter`. `run_renewal_check` must:
///   1. NOT PATCH the CA Secret.
///   2. POST a `Warning` Event with reason `ByoCaExpiringSoon`.
///   3. NOT set a `CaRotationRequired` status condition (BYO = admin's responsibility).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cronjob_byo_ca_expiring_emits_byo_event() {
    let ns = "default";
    let cluster = "c1";

    // Expiring BYO CA (25 days → within 30-day renewal window).
    let byo_ca = generate_cluster_ca("c1-cluster-ca", 25).expect("BYO cluster CA");
    let byo_clients_ca = generate_clients_ca("c1-clients-ca", 365).expect("BYO clients CA");

    // The Kafka CR has generateCertificateAuthority=false on both CAs.
    let kafka_list = {
        let ca_spec = serde_json::json!({
            "generateCertificateAuthority": false,
            "validityDays": 365,
            "renewalDays": 30,
        });
        serde_json::json!({
            "apiVersion": "crabka.io/v1alpha1",
            "kind": "KafkaList",
            "metadata": { "resourceVersion": "1" },
            "items": [{
                "apiVersion": "crabka.io/v1alpha1",
                "kind": "Kafka",
                "metadata": { "name": cluster, "namespace": ns, "uid": "kafka-uid" },
                "spec": {
                    "kafkaVersion": "0.1.1",
                    "clusterCa": ca_spec.clone(),
                    "clientsCa": ca_spec,
                },
                "status": { "conditions": [] }
            }],
        })
    };

    let rules = vec![
        // 1. LIST kafkas
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkas"),
            response: json_response(200, &kafka_list),
        },
        // 2. GET cluster-ca key
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-cluster-ca"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-cluster-ca"),
                    ns,
                    &[("ca.key", &byo_ca.key_pem)],
                ),
            ),
        },
        // 3. GET cluster-ca-cert (expiring)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-cluster-ca-cert"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-cluster-ca-cert"),
                    ns,
                    &[("ca.crt", &byo_ca.cert_pem)],
                ),
            ),
        },
        // 4. GET clients-ca key
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-clients-ca"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-clients-ca"),
                    ns,
                    &[("ca.key", &byo_clients_ca.key_pem)],
                ),
            ),
        },
        // 5. GET clients-ca-cert (fresh)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-clients-ca-cert"),
            response: json_response(
                200,
                &pem_secret(
                    &format!("{cluster}-clients-ca-cert"),
                    ns,
                    &[("ca.crt", &byo_clients_ca.cert_pem)],
                ),
            ),
        },
        // 6. BYO cluster CA is expiring → POST ByoCaExpiringSoon Warning event
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/events"),
            response: json_response(201, &fake_event_body(ns)),
        },
        // 7. Clients CA not expiring (365 days) → no event.
        // 8. GET broker keystore → absent (short-circuit in renew_broker_leafs)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster}-kafka-brokers"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
    ];

    let state = MockState::new(rules);
    let client = mock_client(&state, ns);

    run_renewal_check(client, Some(ns))
        .await
        .expect("renewal check succeeds");

    let observed = state.take_observed();
    let methods_uris: Vec<(Method, String)> = observed
        .iter()
        .map(|r| (r.method().clone(), r.uri().to_string()))
        .collect();

    // Must NOT PATCH any CA Secret.
    let ca_patch = observed.iter().find(|r| {
        r.method() == Method::PATCH
            && r.uri()
                .to_string()
                .contains(&format!("{cluster}-cluster-ca"))
    });
    assert!(
        ca_patch.is_none(),
        "BYO CA must NOT be patched; requests: {methods_uris:?}",
    );

    // Must have emitted a ByoCaExpiringSoon Warning event.
    let event_post = observed.iter().find(|r| {
        r.method() == Method::POST
            && r.uri()
                .to_string()
                .contains(&format!("/namespaces/{ns}/events"))
    });
    assert!(
        event_post.is_some(),
        "expected POST to /events; requests: {methods_uris:?}",
    );
    let event_body: serde_json::Value =
        serde_json::from_slice(event_post.unwrap().body()).expect("event body is JSON");
    assert_eq!(
        (event_body["reason"].as_str(), event_body["type"].as_str()),
        (Some("ByoCaExpiringSoon"), Some("Warning")),
        "body = {event_body}"
    );

    // Must NOT have PATCHed Kafka status (no CaRotationRequired condition for BYO).
    let status_patch = observed.iter().find(|r| {
        r.method() == Method::PATCH
            && r.uri()
                .to_string()
                .contains(&format!("/kafkas/{cluster}/status"))
    });
    assert!(
        status_patch.is_none(),
        "BYO CA must NOT set CaRotationRequired status condition; requests: {methods_uris:?}",
    );

    assert!(state.remaining_rules() == 0, "all rules consumed");
}
