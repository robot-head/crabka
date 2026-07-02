#![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]
//! Integration tests for operator-managed
//! oauth-jwks-trust Secret lifecycle. Verifies the full reconcile path:
//! source Secret reads, PEM concatenation, managed Secret upsert,
//! failure-mode status conditions, and pod-template volume/mount.
//!
//! T3's unit tests in controller/kafka.rs cover the helper's no-op
//! short-circuit paths (no canonical / empty trust certs). T6's
//! integration tests exercise the Secret-touching code paths and the
//! status-condition wiring end-to-end.

use std::sync::Arc;

use assert2::assert;
use base64::Engine as _;
use crabka_operator::{
    controller::{
        kafka::reconcile as reconcile_kafka, kafka_node_pool::reconcile as reconcile_pool,
    },
    crd::{
        Kafka, KafkaSpec, Listener, ListenerAuthentication, ListenerAuthenticationOAuth,
        ListenerType, TlsTrustedCertificate,
    },
};
use http::Method;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, assert_ready_false_with_reason, build_ctx, fake_pool_list_item, happy_path_rules,
    json_response, pool_cr, pool_reconcile_rules, rule_get_secret, rule_get_secret_404,
    rules_for_failure_path,
};

// ── fixtures ────────────────────────────────────────────────────────────────

const FAKE_PEM_1: &[u8] = b"-----BEGIN CERTIFICATE-----\nAAA1\n-----END CERTIFICATE-----";
const FAKE_PEM_2: &[u8] = b"-----BEGIN CERTIFICATE-----\nBBB2\n-----END CERTIFICATE-----";

/// Build a Kafka CR with a single OAuth listener whose
/// `tls_trusted_certificates` references the supplied `(secret, key)` pairs.
fn kafka_with_oauth_trust(name: &str, ns: &str, trust_certs: Vec<(&str, &str)>) -> Kafka {
    let cfg = ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://iss.example/".into(),
        jwks_endpoint_uri: Some("https://iss.example/jwks".into()),
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: trust_certs
            .into_iter()
            .map(|(s, k)| TlsTrustedCertificate {
                secret_name: s.into(),
                certificate: k.into(),
            })
            .collect(),
        access_token_is_jwt: true,
        introspection_endpoint_uri: None,
        user_info_endpoint_uri: None,
        client_id: None,
        client_secret: None,
        introspection_http_timeout_seconds: None,
        max_seconds_without_reauthentication: None,
        valid_token_type: None,
        fallback_user_name_claim: None,
        fallback_user_name_prefix: None,
        groups_claim: None,
        groups_claim_delimiter: None,
        jwks_min_refresh_pause_seconds: None,
        jwks_expiry_seconds: None,
        jwks_ignore_key_use: None,
    };
    let listener = Listener {
        name: "oauth".into(),
        port: 9095,
        type_: ListenerType::Internal,
        tls: true,
        authentication: Some(ListenerAuthentication::OAuth(cfg)),
        configuration: None,
        network_policy_peers: None,
    };
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners: vec![listener],
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
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// JSON body shaped like a `core/v1/Secret` with one base64-encoded data
/// key. Used as the GET response for source-Secret reads inside the
/// trust-bundle assembly loop.
fn source_secret_body(name: &str, namespace: &str, key: &str, value: &[u8]) -> serde_json::Value {
    let b64 = base64::engine::general_purpose::STANDARD.encode(value);
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": format!("{name}-uid") },
        "type": "Opaque",
        "data": { key: b64 },
    })
}

/// JSON body shaped like a `core/v1/Secret` with **no** `data` field —
/// used to exercise the `MissingOauthTrustKey` branch (Secret exists
/// but lacks the named key).
fn source_secret_body_no_data(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": format!("{name}-uid") },
        "type": "Opaque",
    })
}

/// Minimal managed-Secret response body for the trust-bundle PATCH. The
/// reconciler only needs the response to deserialize back into a Secret;
/// echo the same name/namespace.
fn managed_secret_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "managed-uid" },
        "type": "Opaque",
        "data": {}
    })
}

/// Build a rule for `PATCH /secrets/<name>` returning a managed-Secret body.
fn rule_patch_managed_secret(name: &str, namespace: &str) -> MockRule {
    MockRule {
        method: Method::PATCH,
        path_substr: format!("/secrets/{name}"),
        response: json_response(200, &managed_secret_body(name, namespace)),
    }
}

/// Find the managed-Secret PATCH body and decode its `data.ca.crt` key.
fn extract_managed_ca_crt(
    observed: &[http::Request<hyper::body::Bytes>],
    managed_name: &str,
) -> Vec<u8> {
    let patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/secrets/{managed_name}"))
        })
        .unwrap_or_else(|| {
            panic!(
                "managed Secret PATCH for {managed_name} not found; observed: {:?}",
                observed
                    .iter()
                    .map(|r| format!("{} {}", r.method(), r.uri()))
                    .collect::<Vec<_>>()
            )
        });
    let body: serde_json::Value =
        serde_json::from_slice(patch.body()).expect("managed Secret PATCH body is JSON");
    let b64 = body["data"]["ca.crt"]
        .as_str()
        .unwrap_or_else(|| panic!("data.ca.crt missing; body = {body}"));
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .expect("ca.crt decodes as base64")
}

// ── test 1: happy path — two source Secrets concat with newline glue ───────

/// Two source Secrets each with one PEM. After reconcile, the managed
/// Secret's `data.ca.crt` is `base64(PEM1 + "\n" + PEM2)` (since
/// neither source PEM ends in newline). The leading-PEM-doesn't-end-
/// in-newline branch is the one the implementation actually inserts a
/// `\n` glue char into.
#[tokio::test]
async fn oauth_trust_creates_managed_secret_from_concatenated_pems() {
    let items = vec![fake_pool_list_item("brokers", "n1", "c1", 1, 1)];
    let mut rules = happy_path_rules("c1", "n1", &items);
    // Trust-bundle reads happen between the CA pair PATCHes and the
    // pool list GET. FIFO substring matching means insertion order
    // among non-overlapping substrings is irrelevant; just push the
    // source-secret GETs + managed-Secret PATCH onto the rule list.
    rules.push(rule_get_secret(
        "idp-ca-a",
        &source_secret_body("idp-ca-a", "n1", "ca.crt", FAKE_PEM_1),
    ));
    rules.push(rule_get_secret(
        "idp-ca-b",
        &source_secret_body("idp-ca-b", "n1", "ca.crt", FAKE_PEM_2),
    ));
    rules.push(rule_patch_managed_secret("c1-oauth-jwks-trust", "n1"));

    let (ctx, state) = build_ctx("n1", rules);
    let kafka = kafka_with_oauth_trust(
        "c1",
        "n1",
        vec![("idp-ca-a", "ca.crt"), ("idp-ca-b", "ca.crt")],
    );
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let bundle = extract_managed_ca_crt(&observed, "c1-oauth-jwks-trust");

    let mut expected = Vec::new();
    expected.extend_from_slice(FAKE_PEM_1);
    expected.push(b'\n');
    expected.extend_from_slice(FAKE_PEM_2);
    assert!(
        bundle == expected,
        "managed ca.crt must be PEM1 + '\\n' + PEM2"
    );
}

// ── test 2: missing source Secret → MissingOauthTrustSecret ────────────────

/// Source Secret entirely absent (mock returns 404 on the `get_opt`).
/// After reconcile, the Kafka CR status PATCH carries a `Ready`
/// condition with `status: "False"` and `reason: "MissingOauthTrustSecret"`.
#[tokio::test]
async fn oauth_trust_missing_source_secret_rejects_with_missing_oauth_trust_secret() {
    let mut rules = rules_for_failure_path("c2", "n2");
    rules.push(rule_get_secret_404("idp-ca-missing"));

    let (ctx, state) = build_ctx("n2", rules);
    let kafka = kafka_with_oauth_trust("c2", "n2", vec![("idp-ca-missing", "ca.crt")]);
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_ready_false_with_reason(&observed, "c2", "MissingOauthTrustSecret");

    // No managed-Secret PATCH on a failure path.
    assert!(
        !observed.iter().any(|r| r.method() == Method::PATCH
            && r.uri().to_string().contains("/secrets/c2-oauth-jwks-trust")),
        "managed Secret must not be PATCHed on MissingOauthTrustSecret",
    );
}

// ── test 3: Secret present but key absent → MissingOauthTrustKey ───────────

/// Secret exists but lacks the named key. Reason: `MissingOauthTrustKey`.
#[tokio::test]
async fn oauth_trust_missing_key_in_source_secret_rejects_with_missing_oauth_trust_key() {
    let mut rules = rules_for_failure_path("c3", "n3");
    rules.push(rule_get_secret(
        "idp-ca-keyless",
        &source_secret_body_no_data("idp-ca-keyless", "n3"),
    ));

    let (ctx, state) = build_ctx("n3", rules);
    let kafka = kafka_with_oauth_trust("c3", "n3", vec![("idp-ca-keyless", "ca.crt")]);
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_ready_false_with_reason(&observed, "c3", "MissingOauthTrustKey");
}

// ── test 4: Secret + key present but value zero bytes → EmptyOauthTrustValue ─

/// Secret + key exist; value is zero bytes. Reason: `EmptyOauthTrustValue`.
#[tokio::test]
async fn oauth_trust_empty_key_value_rejects_with_empty_oauth_trust_value() {
    let mut rules = rules_for_failure_path("c4", "n4");
    rules.push(rule_get_secret(
        "idp-ca-empty",
        &source_secret_body("idp-ca-empty", "n4", "ca.crt", b""),
    ));

    let (ctx, state) = build_ctx("n4", rules);
    let kafka = kafka_with_oauth_trust("c4", "n4", vec![("idp-ca-empty", "ca.crt")]);
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_ready_false_with_reason(&observed, "c4", "EmptyOauthTrustValue");
}

// ── test 5: empty tls_trusted_certificates → no managed Secret ─────────────

/// An OAuth listener with empty `tls_trusted_certificates` short-
/// circuits inside `reconcile_oauth_jwks_trust` and never touches the
/// Secret API. After reconcile, the observed-request log carries NO
/// PATCH against `*-oauth-jwks-trust`.
#[tokio::test]
async fn oauth_trust_no_trust_certs_does_not_create_managed_secret() {
    let items = vec![fake_pool_list_item("brokers", "n5", "c5", 1, 1)];
    let rules = happy_path_rules("c5", "n5", &items);

    let (ctx, state) = build_ctx("n5", rules);
    let kafka = kafka_with_oauth_trust("c5", "n5", vec![]);
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert!(
        !observed
            .iter()
            .any(|r| r.uri().to_string().contains("/secrets/c5-oauth-jwks-trust")),
        "no trust certs → no managed Secret traffic; observed: {:?}",
        observed
            .iter()
            .map(|r| format!("{} {}", r.method(), r.uri()))
            .collect::<Vec<_>>(),
    );
}

// ── test 6: source-rotation re-renders the managed Secret ──────────────────

/// Reconcile once with the source Secret carrying value A → managed
/// Secret's `data.ca.crt == base64(A)`. Reconcile again (separate
/// mock + ctx) with the source carrying value B → managed Secret's
/// `data.ca.crt == base64(B)`. Verifies the operator re-derives the
/// bundle on every reconcile pass (no stale-input caching).
#[tokio::test]
async fn oauth_trust_managed_secret_updates_when_source_changes() {
    const PEM_A: &[u8] = b"-----BEGIN CERTIFICATE-----\nROTATION-A\n-----END CERTIFICATE-----";
    const PEM_B: &[u8] = b"-----BEGIN CERTIFICATE-----\nROTATION-B\n-----END CERTIFICATE-----";

    // ── pass 1: source value A ────────────────────────────────────────
    let items = vec![fake_pool_list_item("brokers", "n6", "c6", 1, 1)];
    let mut rules_a = happy_path_rules("c6", "n6", &items);
    rules_a.push(rule_get_secret(
        "idp-ca-rot",
        &source_secret_body("idp-ca-rot", "n6", "ca.crt", PEM_A),
    ));
    rules_a.push(rule_patch_managed_secret("c6-oauth-jwks-trust", "n6"));

    let (ctx_a, state_a) = build_ctx("n6", rules_a);
    let kafka_a = kafka_with_oauth_trust("c6", "n6", vec![("idp-ca-rot", "ca.crt")]);
    reconcile_kafka(Arc::new(kafka_a), ctx_a).await.unwrap();
    let observed_a = state_a.take_observed();
    let bundle_a = extract_managed_ca_crt(&observed_a, "c6-oauth-jwks-trust");
    assert!(bundle_a == PEM_A);

    // ── pass 2: source value B (fresh mock, rotated source) ───────────
    let items = vec![fake_pool_list_item("brokers", "n6", "c6", 1, 1)];
    let mut rules_b = happy_path_rules("c6", "n6", &items);
    rules_b.push(rule_get_secret(
        "idp-ca-rot",
        &source_secret_body("idp-ca-rot", "n6", "ca.crt", PEM_B),
    ));
    rules_b.push(rule_patch_managed_secret("c6-oauth-jwks-trust", "n6"));

    let (ctx_b, state_b) = build_ctx("n6", rules_b);
    let kafka_b = kafka_with_oauth_trust("c6", "n6", vec![("idp-ca-rot", "ca.crt")]);
    reconcile_kafka(Arc::new(kafka_b), ctx_b).await.unwrap();
    let observed_b = state_b.take_observed();
    let bundle_b = extract_managed_ca_crt(&observed_b, "c6-oauth-jwks-trust");
    assert!(bundle_b == PEM_B);

    assert!(
        bundle_a != bundle_b,
        "rotated source must produce a rotated managed bundle"
    );
}

// ── pool-reconcile fixtures (tests 7 + 8) ──────────────────────────────────

/// Parent-Kafka body that carries an OAuth listener with
/// `tls_trusted_certificates` populated. Used as the GET response for
/// the pool reconciler's `kafka_api.get_opt(parent_name)` step so the
/// rendered pod template picks up the `Some(...)` branch of
/// `oauth_jwks_trust_secret_name`.
fn parent_kafka_body_with_oauth_trust(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": {
            "kafkaVersion": "0.1.1",
            "listeners": [{
                "name": "oauth",
                "port": 9095,
                "type": "internal",
                "tls": true,
                "authentication": {
                    "type": "oauth",
                    "validIssuerUri": "https://iss.example/",
                    "jwksEndpointUri": "https://iss.example/jwks",
                    "tlsTrustedCertificates": [
                        { "secretName": "idp-ca", "certificate": "ca.crt" }
                    ]
                }
            }]
        },
        "status": cleared_version_status()
    })
}

/// Parent-Kafka body with **no** listeners — pool reconciler sees no
/// OAuth listener and skips the trust-bundle volume entirely.
fn parent_kafka_body_no_listeners(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": { "kafkaVersion": "0.1.1" },
        "status": cleared_version_status()
    })
}

/// A reconciled parent's cleared version model: the pool reconciler gates
/// pod creation on `KafkaVersionValid=True` / a finalized metadata version
/// (see `kafka_node_pool::version_gate`), so a parent fed to the pool
/// reconciler must look like a validated cluster.
fn cleared_version_status() -> serde_json::Value {
    serde_json::json!({
        "conditions": [{
            "type": "KafkaVersionValid",
            "status": "True",
            "reason": "Valid",
            "message": "kafkaVersion 0.1.1 metadata.version 0.1",
            "lastTransitionTime": "2026-05-22T00:00:00Z"
        }],
        "metadataVersion": "0.1"
    })
}

// ── test 7: StatefulSet mounts the managed trust Secret when present ───────

/// Full pool reconcile with a parent Kafka CR that carries an OAuth
/// listener + `tls_trusted_certificates`. Capture the StatefulSet
/// PATCH body and assert it contains both the `oauth-jwks-trust`
/// pod volume (sourcing `<parent>-oauth-jwks-trust`) and the matching
/// `volumeMount` at `/etc/crabka/oauth-jwks-trust` (readOnly).
#[tokio::test]
async fn statefulset_mounts_oauth_jwks_trust_secret_when_trust_certs_present() {
    let rules = pool_reconcile_rules(
        "c7",
        "brokers",
        "n7",
        &parent_kafka_body_with_oauth_trust("c7", "n7"),
    );
    let (ctx, state) = build_ctx("n7", rules);
    let pool = pool_cr("brokers", "n7", "c7", 1);
    reconcile_pool(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/statefulsets/c7-brokers")
        })
        .expect("StatefulSet PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS body is JSON");
    let pod_spec = &body["spec"]["template"]["spec"];

    // Volume entry sources the managed Secret.
    let volumes = pod_spec["volumes"]
        .as_array()
        .expect("volumes array present");
    let trust_vol = volumes
        .iter()
        .find(|v| v["name"] == "oauth-jwks-trust")
        .unwrap_or_else(|| panic!("oauth-jwks-trust volume present; body = {body}"));
    assert!(
        trust_vol["secret"]["secretName"] == "c7-oauth-jwks-trust",
        "managed Secret name; body = {body}"
    );

    // VolumeMount on the broker container.
    let containers = pod_spec["containers"]
        .as_array()
        .expect("containers array present");
    let broker = containers
        .iter()
        .find(|c| c["name"] == "broker")
        .unwrap_or_else(|| panic!("broker container present; body = {body}"));
    let mounts = broker["volumeMounts"]
        .as_array()
        .expect("volumeMounts array present");
    let trust_mount = mounts
        .iter()
        .find(|m| m["name"] == "oauth-jwks-trust")
        .unwrap_or_else(|| panic!("oauth-jwks-trust mount present; body = {body}"));
    assert!(
        trust_mount["mountPath"] == "/etc/crabka/oauth-jwks-trust",
        "mount path contract; body = {body}"
    );
    assert!(
        trust_mount["readOnly"] == true,
        "trust mount must be readOnly; body = {body}"
    );
}

// ── test 8: StatefulSet omits the trust volume/mount when no trust certs ──

/// Same pool fixture, but the parent Kafka CR carries no listeners
/// (and therefore no OAuth trust certs). The StatefulSet PATCH body
/// must NOT carry the `oauth-jwks-trust` volume or mount.
#[tokio::test]
async fn statefulset_omits_oauth_jwks_trust_volume_when_no_trust_certs() {
    let rules = pool_reconcile_rules(
        "c8",
        "brokers",
        "n8",
        &parent_kafka_body_no_listeners("c8", "n8"),
    );
    let (ctx, state) = build_ctx("n8", rules);
    let pool = pool_cr("brokers", "n8", "c8", 1);
    reconcile_pool(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/statefulsets/c8-brokers")
        })
        .expect("StatefulSet PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS body is JSON");
    let pod_spec = &body["spec"]["template"]["spec"];

    let volumes = pod_spec["volumes"]
        .as_array()
        .expect("volumes array present");
    assert!(
        volumes.iter().all(|v| v["name"] != "oauth-jwks-trust"),
        "no OAuth listener → no oauth-jwks-trust pod volume; body = {body}",
    );

    let containers = pod_spec["containers"]
        .as_array()
        .expect("containers array present");
    let broker = containers
        .iter()
        .find(|c| c["name"] == "broker")
        .unwrap_or_else(|| panic!("broker container present; body = {body}"));
    let mounts = broker["volumeMounts"]
        .as_array()
        .expect("volumeMounts array present");
    assert!(
        mounts.iter().all(|m| m["name"] != "oauth-jwks-trust"),
        "no OAuth listener → no oauth-jwks-trust mount; body = {body}",
    );
}
