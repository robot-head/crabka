//! Integration tests for the `oauth` listener authentication variant.
//! Verifies the full reconcile path against the kube-mock
//! harness: broker-config `ConfigMap` contents (`[oauthbearer]` TOML block
//! + per-listener `sasl_config`), `ListenersValid` status conditions
//!   patched on per-listener validation failures, cross-listener config-
//!   conflict detection, and the `WeakAuth` Event emission on http:// JWKS.
//!
//! T3's unit tests inside `controller/listeners.rs` already cover the
//! pure validator + TOML render functions in isolation. The added value
//! at this layer is verifying that the validator's `Err` becomes a
//! correctly-shaped `Status.conditions[]` entry (mirroring the
//! `listener_mtls_requires_tls_validation_error_surfaces_status` test in
//! `reconcile_listener_auth.rs`) and that the rendered TOML actually
//! lands in the broker-config `ConfigMap`.

use std::sync::Arc;

use assert2::{assert, check};
use crabka_operator::{
    controller::kafka::reconcile,
    crd::{
        Kafka, KafkaSpec, Listener, ListenerAuthentication, ListenerAuthenticationOAuth,
        ListenerType, OauthClientSecretRef, TlsTrustedCertificate,
    },
};
use http::Method;

#[path = "shared/mod.rs"]
mod shared;

use shared::{MockRule, build_ctx, extract_broker0_toml, happy_path_rules, json_response};

// ── helpers ──────────────────────────────────────────────────────────────────

fn kafka_cr_with_listeners(name: &str, namespace: &str, listeners: Vec<Listener>) -> Kafka {
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

fn oauth_listener(name: &str, port: i32, tls: bool, cfg: ListenerAuthenticationOAuth) -> Listener {
    Listener {
        name: name.into(),
        port,
        type_: ListenerType::Internal,
        tls,
        authentication: Some(ListenerAuthentication::OAuth(Box::new(cfg))),
        configuration: None,
        network_policy_peers: None,
    }
}

/// Minimal OAuth config with only the two required fields populated.
fn oauth_cfg_minimal() -> ListenerAuthenticationOAuth {
    ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://issuer.example.com/".into(),
        jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
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
    }
}

/// Full-config OAuth listener used to verify every optional `[oauthbearer]`
/// key reaches the rendered `ConfigMap`.
fn oauth_cfg_full() -> ListenerAuthenticationOAuth {
    ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://kc.example.com/realms/kafka".into(),
        jwks_endpoint_uri: Some(
            "https://kc.example.com/realms/kafka/protocol/openid-connect/certs".into(),
        ),
        valid_audience: Some("kafka".into()),
        user_name_claim: Some("preferred_username".into()),
        custom_claim_check: Some("$.scope[?@ == 'kafka.write']".into()),
        jwks_refresh_seconds: Some(300),
        max_clock_skew_seconds: Some(30),
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
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
    }
}

/// Find the `ListenersValid` condition in the status PATCH and assert
/// `status: "False"` with the expected `reason`. Mirrors the
/// `listener_mtls_requires_tls_validation_error_surfaces_status` shape.
fn assert_listeners_invalid_with_reason(
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

    let valid = conds
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid present; body = {body}"));
    check!(valid["status"] == "False", "body = {body}");
    check!(valid["reason"] == expected_reason, "body = {body}");

    // The ConfigMap PATCH must be absent on the validation-fail path.
    check!(
        !observed.iter().any(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/configmaps/{cluster}-broker-config"))
        }),
        "validation failure must not patch the broker-config ConfigMap"
    );
}

/// Build a `MockRule` set for a reconcile that fails listener validation:
/// drop the `ConfigMap` + broker-keystore rules from `happy_path_rules`,
/// since neither call fires when validation rejects the spec.
fn rules_for_invalid_listeners(
    name: &str,
    namespace: &str,
    pool_items: &[serde_json::Value],
) -> Vec<MockRule> {
    let mut rules = happy_path_rules(name, namespace, pool_items);
    rules.retain(|r| !r.path_substr.contains("/configmaps/"));
    rules.retain(|r| !r.path_substr.contains("-kafka-brokers"));
    rules
}

/// Minimal Event response body for the `WeakAuth` event POST. Mirrors
/// `ca_renewal_cronjob::fake_event_body`.
fn fake_event_body(namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": { "name": "crabka-listener-auth-abc", "namespace": namespace, "uid": "event-uid" },
        "involvedObject": {},
        "message": "test event",
        "reason": "WeakAuth",
        "type": "Warning",
        "reportingComponent": "crabka-operator/listener-auth-check",
        "reportingInstance": "crabka-operator-renewal",
        "eventTime": null,
    })
}

// ── test 1: full-config oauth listener renders the [oauthbearer] block ──────

/// Reconciling a `Kafka` CR with a fully-populated OAuth listener
/// causes the broker-config `ConfigMap` to embed the broker-global
/// `[oauthbearer]` TOML block with every optional key set.
#[tokio::test]
async fn oauth_listener_renders_oauthbearer_toml_block() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns1", "c1", 1, 1)];
    let (ctx, state) = build_ctx("ns1", happy_path_rules("c1", "ns1", &items));

    let kafka = kafka_cr_with_listeners(
        "c1",
        "ns1",
        vec![oauth_listener("oauth", 9095, true, oauth_cfg_full())],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c1");

    for needle in [
        "[oauthbearer]",
        "jwks_endpoint_uri = \"https://kc.example.com/realms/kafka/protocol/openid-connect/certs\"",
        "valid_issuer_uri = \"https://kc.example.com/realms/kafka\"",
        "expected_audience = \"kafka\"",
        "principal_claim_name = \"preferred_username\"",
        "custom_claim_check = '''$.scope[?@ == 'kafka.write']'''",
        "jwks_refresh_interval_ms = 300000",
        "allowable_clock_skew_ms = 30000",
    ] {
        assert!(toml.contains(needle), "missing {needle:?} in TOML: {toml}");
    }
}

// ── test 2: OAUTHBEARER appears in the listener's sasl_mechanisms ───────────

/// An OAuth listener with `enableOauthBearer: true` (the default) must
/// carry `sasl_config = { enabled_mechanisms = ["OAUTHBEARER"] }` in its
/// per-listener TOML row.
#[tokio::test]
async fn oauth_listener_appends_oauthbearer_to_sasl_mechanisms() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns2", "c2", 1, 1)];
    let (ctx, state) = build_ctx("ns2", happy_path_rules("c2", "ns2", &items));

    let kafka = kafka_cr_with_listeners(
        "c2",
        "ns2",
        vec![oauth_listener("oauth", 9095, true, oauth_cfg_minimal())],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c2");

    assert!(
        toml.contains("sasl_config = { enabled_mechanisms = [\"OAUTHBEARER\"] }"),
        "TOML: {toml}"
    );
    assert!(toml.contains("protocol = \"SaslSsl\""), "TOML: {toml}");
}

// ── test 3: enable_oauth_bearer=false keeps the block, drops the mechanism ──

/// `enableOauthBearer: false` keeps the broker-global `[oauthbearer]`
/// block (the broker still validates incoming OAuth tokens) but the
/// listener's `sasl_config` is omitted — no `OAUTHBEARER` advertised
/// over the wire on that listener. Symmetric with Strimzi.
#[tokio::test]
async fn oauth_listener_with_enable_false_omits_mechanism_but_keeps_config_block() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns3", "c3", 1, 1)];
    let (ctx, state) = build_ctx("ns3", happy_path_rules("c3", "ns3", &items));

    let mut cfg = oauth_cfg_full();
    cfg.enable_oauth_bearer = false;
    let kafka =
        kafka_cr_with_listeners("c3", "ns3", vec![oauth_listener("oauth", 9095, true, cfg)]);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c3");

    assert!(toml.contains("[oauthbearer]"), "TOML: {toml}");
    assert!(!toml.contains("sasl_config"), "TOML: {toml}");
}

// ── test 4: oauth without transport TLS → ListenersValid=False ──────────────

/// `tls: false` on an OAuth listener fails validation. The reconcile
/// must patch `ListenersValid=False` with reason
/// `ListenerOauthRequiresTransportTls`, and no `ConfigMap` PATCH must
/// occur. This duplicates T3's
/// `validate_listeners_rejects_oauth_without_tls` unit test at the
/// validator level — the added value here is verifying the wiring of
/// the validator's `Err` into the patched status condition.
#[tokio::test]
async fn oauth_listener_without_tls_rejected_with_listeners_valid_false() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns4", "c4", 1, 1)];
    let rules = rules_for_invalid_listeners("c4", "ns4", &items);
    let (ctx, state) = build_ctx("ns4", rules);

    let kafka = kafka_cr_with_listeners(
        "c4",
        "ns4",
        vec![oauth_listener("oauth", 9095, false, oauth_cfg_minimal())],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(&observed, "c4", "ListenerOauthRequiresTransportTls");
}

// ── test 5: http:// JWKS reconciles, but emits a WeakAuth Event ─────────────

/// `jwks_endpoint_uri: http://...` is accepted by the validator
/// (T3 explicitly relaxed this so e2e against an in-cluster Keycloak
/// works without TLS-terminating the `IdP`), but the reconciler must
/// emit a `Warning` Event with reason `WeakAuth` on the `Kafka` CR.
/// Verifies the POST to `/events` is captured and the event payload
/// carries `reason: WeakAuth`.
#[tokio::test]
async fn oauth_listener_with_http_jwks_uri_reconciles_but_emits_weak_auth_event() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns5", "c5", 1, 1)];
    let mut rules = happy_path_rules("c5", "ns5", &items);
    // Event POST happens before the rest of the reconcile and is `.ok()`-ed,
    // so a 404 would not fail the reconcile — but adding an explicit rule
    // lets the test assert the POST body.
    rules.insert(
        0,
        MockRule {
            method: Method::POST,
            path_substr: "/namespaces/ns5/events".into(),
            response: json_response(201, &fake_event_body("ns5")),
        },
    );
    let (ctx, state) = build_ctx("ns5", rules);

    let mut cfg = oauth_cfg_minimal();
    cfg.jwks_endpoint_uri = Some("http://issuer.example.com/jwks".into());
    let kafka =
        kafka_cr_with_listeners("c5", "ns5", vec![oauth_listener("oauth", 9095, true, cfg)]);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // Find the Event POST.
    let event_post = observed
        .iter()
        .find(|r| {
            r.method() == Method::POST && r.uri().to_string().contains("/namespaces/ns5/events")
        })
        .unwrap_or_else(|| {
            panic!(
                "expected POST to /namespaces/ns5/events; observed: {:?}",
                observed
                    .iter()
                    .map(|r| format!("{} {}", r.method(), r.uri()))
                    .collect::<Vec<_>>()
            )
        });
    let body: serde_json::Value =
        serde_json::from_slice(event_post.body()).expect("event body is JSON");
    assert!(body["reason"] == "WeakAuth", "event body = {body}");
    assert!(body["type"] == "Warning", "event body = {body}");
    let msg = body["message"]
        .as_str()
        .unwrap_or_else(|| panic!("event message missing; body = {body}"));
    assert!(
        msg.contains("http://") && msg.contains("oauth"),
        "WeakAuth message must mention http:// + listener name; got: {msg}"
    );

    // Reconcile must still succeed — Ready=True path. Verify the ConfigMap
    // PATCH was issued (proves we made it past validation).
    let _ = extract_broker0_toml(&observed, "c5");
}

// ── test 6: ftp:// JWKS rejected ────────────────────────────────────────────

/// Non-http(s) JWKS URI schemes are rejected with reason
/// `ListenerOauthInvalidUri`. Duplicates T3's
/// `validate_listeners_rejects_oauth_with_ftp_jwks_uri` at the unit
/// level; this asserts the status condition surface.
#[tokio::test]
async fn oauth_listener_with_ftp_jwks_uri_rejected() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns6", "c6", 1, 1)];
    let rules = rules_for_invalid_listeners("c6", "ns6", &items);
    let (ctx, state) = build_ctx("ns6", rules);

    let mut cfg = oauth_cfg_minimal();
    cfg.jwks_endpoint_uri = Some("ftp://issuer.example.com/jwks".into());
    let kafka =
        kafka_cr_with_listeners("c6", "ns6", vec![oauth_listener("oauth", 9095, true, cfg)]);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(&observed, "c6", "ListenerOauthInvalidUri");
}

// ── test 7: empty issuer URI rejected ───────────────────────────────────────

/// An empty `validIssuerUri` is rejected with reason
/// `ListenerOauthInvalidUri`. Duplicates T3's
/// `validate_listeners_rejects_oauth_with_empty_issuer_uri`.
#[tokio::test]
async fn oauth_listener_with_empty_issuer_uri_rejected() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns7", "c7", 1, 1)];
    let rules = rules_for_invalid_listeners("c7", "ns7", &items);
    let (ctx, state) = build_ctx("ns7", rules);

    let mut cfg = oauth_cfg_minimal();
    cfg.valid_issuer_uri = String::new();
    let kafka =
        kafka_cr_with_listeners("c7", "ns7", vec![oauth_listener("oauth", 9095, true, cfg)]);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(&observed, "c7", "ListenerOauthInvalidUri");
}

// ── test 8: jwks_refresh < 30 rejected ──────────────────────────────────────

/// `jwksRefreshSeconds: 29` is rejected with reason
/// `ListenerOauthInvalidRefresh`. Duplicates T3's
/// `validate_listeners_rejects_oauth_with_short_jwks_refresh`.
#[tokio::test]
async fn oauth_listener_with_jwks_refresh_below_30_rejected() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns8", "c8", 1, 1)];
    let rules = rules_for_invalid_listeners("c8", "ns8", &items);
    let (ctx, state) = build_ctx("ns8", rules);

    let mut cfg = oauth_cfg_minimal();
    cfg.jwks_refresh_seconds = Some(29);
    let kafka =
        kafka_cr_with_listeners("c8", "ns8", vec![oauth_listener("oauth", 9095, true, cfg)]);
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(&observed, "c8", "ListenerOauthInvalidRefresh");
}

// No test for the legacy
// `ListenerOauthInvalidScope` / `OAuthCustomClaimCheckScopeEmpty`
// variant — `customClaimCheck` is a free-form JsonPath
// string and CRD `minLength: 1` rejects empty values at admission
// before the operator ever sees them.

// ── test 10: two oauth listeners with identical config → Ready ──────────────

/// Two OAuth listeners with identical `[oauthbearer]` config dedup
/// cleanly (per `oauth_canonical` in `listeners.rs`) and reconcile to
/// `Ready=True`. The rendered TOML carries one `[oauthbearer]` block
/// and `OAUTHBEARER` on both per-listener `sasl_config` rows.
#[tokio::test]
async fn two_oauth_listeners_with_identical_config_reconcile_clean() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns10", "c10", 1, 1)];
    let (ctx, state) = build_ctx("ns10", happy_path_rules("c10", "ns10", &items));

    let cfg = oauth_cfg_full();
    let kafka = kafka_cr_with_listeners(
        "c10",
        "ns10",
        vec![
            oauth_listener("oauth-a", 9095, true, cfg.clone()),
            oauth_listener("oauth-b", 9096, true, cfg),
        ],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c10");

    // Exactly one [oauthbearer] block.
    assert!(
        toml.matches("[oauthbearer]").count() == 1,
        "expected exactly one [oauthbearer] block; TOML: {toml}"
    );
    // Both listeners advertise OAUTHBEARER.
    assert!(
        toml.matches("sasl_config = { enabled_mechanisms = [\"OAUTHBEARER\"] }")
            .count()
            == 2,
        "both listeners must advertise OAUTHBEARER; TOML: {toml}"
    );

    // Ready=True wiring.
    let status_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/c10/status"))
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let valid = body["status"]["conditions"]
        .as_array()
        .expect("conditions array")
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid present; body = {body}"));
    assert!(valid["status"] == "True", "body = {body}");
}

// ── test 11: two oauth listeners differing only in enable_oauth_bearer ──────

/// `oauth_canonical` masks `enable_oauth_bearer`, so two OAuth listeners
/// that differ only in that bit are considered equivalent for the
/// cross-listener conflict check and reconcile cleanly. The rendered
/// TOML still emits `sasl_config` only on the enable=true listener.
#[tokio::test]
async fn two_oauth_listeners_differing_only_in_enable_oauth_bearer_reconcile_clean() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns11", "c11", 1, 1)];
    let (ctx, state) = build_ctx("ns11", happy_path_rules("c11", "ns11", &items));

    let mut cfg_a = oauth_cfg_full();
    cfg_a.enable_oauth_bearer = true;
    let mut cfg_b = oauth_cfg_full();
    cfg_b.enable_oauth_bearer = false;
    let kafka = kafka_cr_with_listeners(
        "c11",
        "ns11",
        vec![
            oauth_listener("oauth-a", 9095, true, cfg_a),
            oauth_listener("oauth-b", 9096, true, cfg_b),
        ],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c11");

    // Single broker-global [oauthbearer] block.
    assert!(
        toml.matches("[oauthbearer]").count() == 1,
        "expected exactly one [oauthbearer] block; TOML: {toml}"
    );
    // Only the enable=true listener advertises the mechanism.
    assert!(
        toml.matches("sasl_config = { enabled_mechanisms = [\"OAUTHBEARER\"] }")
            .count()
            == 1,
        "only the enable=true listener must advertise OAUTHBEARER; TOML: {toml}"
    );

    // Ready=True wiring.
    let status_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/c11/status"))
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let valid = body["status"]["conditions"]
        .as_array()
        .expect("conditions array")
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid present; body = {body}"));
    assert!(valid["status"] == "True", "body = {body}");
}

// ── test 12: divergent oauth configs rejected with ConflictingOAuthConfig ───

/// Two OAuth listeners with different `validAudience` values cannot
/// share the broker-global `[oauthbearer]` block. The reconciler must
/// patch `ListenersValid=False` with reason `ConflictingOAuthConfig`.
#[tokio::test]
async fn two_oauth_listeners_with_divergent_config_rejected_with_conflicting_oauth_config() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns12", "c12", 1, 1)];
    let rules = rules_for_invalid_listeners("c12", "ns12", &items);
    let (ctx, state) = build_ctx("ns12", rules);

    let mut cfg_a = oauth_cfg_full();
    cfg_a.valid_audience = Some("kafka-a".into());
    let mut cfg_b = oauth_cfg_full();
    cfg_b.valid_audience = Some("kafka-b".into());
    let kafka = kafka_cr_with_listeners(
        "c12",
        "ns12",
        vec![
            oauth_listener("oauth-a", 9095, true, cfg_a),
            oauth_listener("oauth-b", 9096, true, cfg_b),
        ],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(&observed, "c12", "ConflictingOAuthConfig");
}

// ── test 13: divergent trust-certs rejected with ConflictingOAuthConfig ─────

/// Two OAuth listeners whose `[oauthbearer]` config is
/// identical EXCEPT for `tls_trusted_certificates` (one empty, one
/// pointing at a Secret) cannot share the broker-global block — the
/// trust-bundle is part of the canonical OAuth fingerprint. The
/// reconciler must reject with `ListenersValid=False` reason
/// `ConflictingOAuthConfig`, and (because validation fails before any
/// per-broker rendering) must not patch the broker-config `ConfigMap`.
#[tokio::test]
async fn two_oauth_listeners_with_divergent_trust_certs_rejected_with_conflicting_oauth_config() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns13", "c13", 1, 1)];
    let rules = rules_for_invalid_listeners("c13", "ns13", &items);
    let (ctx, state) = build_ctx("ns13", rules);

    let mut cfg_a = oauth_cfg_full();
    cfg_a.tls_trusted_certificates = vec![];
    let mut cfg_b = oauth_cfg_full();
    cfg_b.tls_trusted_certificates = vec![TlsTrustedCertificate {
        secret_name: "any-secret".into(),
        certificate: "tls.crt".into(),
    }];
    let kafka = kafka_cr_with_listeners(
        "c13",
        "ns13",
        vec![
            oauth_listener("oauth-a", 9095, true, cfg_a),
            oauth_listener("oauth-b", 9096, true, cfg_b),
        ],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(&observed, "c13", "ConflictingOAuthConfig");
}

// ── test 14: trust-certs reconcile end-to-end → idp_tls_trust + managed Secret

/// A single OAuth listener with `tls_trusted_certificates`
/// pointing at a user-supplied Secret must:
///   1. drive `reconcile_oauth_jwks_trust` to GET the source Secret
///      and SSA the managed `{kafka}-oauth-jwks-trust` Secret with the
///      concatenated PEM under key `ca.crt`,
///   2. cause the broker-config `ConfigMap` render path to emit
///      `idp_tls_trust = "/etc/crabka/oauth-jwks-trust/ca.crt"` in
///      `broker-0.toml` (T2's render-path wiring), and
///   3. surface `ListenersValid=True` on the Kafka status (the
///      reconcile path made it past validation + trust assembly with
///      no errors).
///
/// The FIFO mock list inserts the source-Secret GET + managed-Secret
/// PATCH between the pool-list step and the broker-keystore step (the
/// order they fire in `kafka.rs::reconcile`).
#[tokio::test]
async fn oauth_listener_with_tls_trusted_certificates_reconciles_renders_idp_tls_trust_line() {
    use base64::Engine as _;

    let items = vec![shared::fake_pool_list_item("brokers", "ns14", "c14", 1, 1)];
    let mut rules = happy_path_rules("c14", "ns14", &items);

    // Source Secret read by `reconcile_oauth_jwks_trust`. The operator
    // doesn't parse the bytes — any non-empty value under the named key
    // is concatenated into the managed bundle.
    let pem = b"-----BEGIN CERTIFICATE-----\nMIIBIzCCAQ==\n-----END CERTIFICATE-----\n";
    let pem_b64 = base64::engine::general_purpose::STANDARD.encode(pem);
    let source_secret_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "demo-keycloak-ca", "namespace": "ns14", "uid": "src-uid" },
        "type": "Opaque",
        "data": { "tls.crt": pem_b64 },
    });
    let managed_secret_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "c14-oauth-jwks-trust", "namespace": "ns14", "uid": "mgd-uid" },
        "type": "Opaque",
        "data": { "ca.crt": pem_b64 },
    });
    // Insert the trust-assembly rules ahead of the broker-keystore /
    // ConfigMap rules so FIFO picks them up first. (The mock matches on
    // path substring + method; the source GET and managed PATCH have
    // unique substrings, so ordering is purely a defensive measure.)
    rules.insert(
        0,
        MockRule {
            method: Method::GET,
            path_substr: "/secrets/demo-keycloak-ca".into(),
            response: json_response(200, &source_secret_body),
        },
    );
    rules.insert(
        1,
        MockRule {
            method: Method::PATCH,
            path_substr: "/secrets/c14-oauth-jwks-trust".into(),
            response: json_response(200, &managed_secret_body),
        },
    );

    let (ctx, state) = build_ctx("ns14", rules);

    let mut cfg = oauth_cfg_minimal();
    cfg.tls_trusted_certificates = vec![TlsTrustedCertificate {
        secret_name: "demo-keycloak-ca".into(),
        certificate: "tls.crt".into(),
    }];
    let kafka = kafka_cr_with_listeners(
        "c14",
        "ns14",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // (1) Managed Secret SSA fired against the expected URI with the
    //     concatenated PEM bundle under `ca.crt`.
    let managed_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains("/secrets/c14-oauth-jwks-trust")
        })
        .unwrap_or_else(|| {
            panic!(
                "expected SSA PATCH to /secrets/c14-oauth-jwks-trust; observed: {:?}",
                observed
                    .iter()
                    .map(|r| format!("{} {}", r.method(), r.uri()))
                    .collect::<Vec<_>>()
            )
        });
    let mgd_body: serde_json::Value =
        serde_json::from_slice(managed_patch.body()).expect("managed Secret PATCH body is JSON");
    let mgd_b64 = mgd_body["data"]["ca.crt"]
        .as_str()
        .unwrap_or_else(|| panic!("managed Secret data.ca.crt missing; body = {mgd_body}"));
    let mgd_bytes = base64::engine::general_purpose::STANDARD
        .decode(mgd_b64)
        .expect("managed Secret ca.crt is base64");
    assert!(
        mgd_bytes == pem.to_vec(),
        "managed Secret bundle must match source PEM bytes"
    );

    // (2) ConfigMap render contains the idp_tls_trust pointer.
    let toml = extract_broker0_toml(&observed, "c14");
    assert!(
        toml.contains("idp_tls_trust = \"/etc/crabka/oauth-jwks-trust/ca.crt\""),
        "broker-0.toml must reference the mounted trust bundle; TOML: {toml}"
    );

    // (3) ListenersValid=True on the status PATCH.
    let status_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/c14/status"))
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let valid = body["status"]["conditions"]
        .as_array()
        .expect("conditions array")
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid present; body = {body}"));
    assert!(valid["status"] == "True", "body = {body}");
}

// ── test 15: divergent access_token_is_jwt rejected with ConflictingOAuthConfig

/// Two OAuth listeners that each pass per-listener
/// validation but disagree on `accessTokenIsJwt` (one JWT-mode with
/// `jwksEndpointUri`, the other introspection-mode with
/// `introspectionEndpointUri` + `clientId` + `clientSecret`) cannot
/// share the broker-global `[oauthbearer]` block — the canonical
/// fingerprint differs in the JWT-vs-introspection bit and every
/// mode-specific field. The cross-listener canonical guard must
/// reject the combination as `ConflictingOAuthConfig`, and no
/// broker-config `ConfigMap` PATCH must fire.
#[tokio::test]
async fn two_oauth_listeners_with_divergent_access_token_is_jwt_rejected_with_conflicting_oauth_config()
 {
    let items = vec![shared::fake_pool_list_item("brokers", "ns15", "c15", 1, 1)];
    let rules = rules_for_invalid_listeners("c15", "ns15", &items);
    let (ctx, state) = build_ctx("ns15", rules);

    // Listener A: JWT mode — passes per-listener validation (has
    // jwksEndpointUri, no introspection-mode fields).
    let mut cfg_a = oauth_cfg_minimal();
    cfg_a.access_token_is_jwt = true;
    cfg_a.jwks_endpoint_uri = Some("https://idp.example/jwks".into());

    // Listener B: introspection mode — passes per-listener validation
    // (no jwksEndpointUri, has all introspection-mode required fields).
    let cfg_b = ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://idp.example/".into(),
        jwks_endpoint_uri: None,
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
        access_token_is_jwt: false,
        introspection_endpoint_uri: Some("https://idp.example/introspect".into()),
        user_info_endpoint_uri: None,
        client_id: Some("kafka-broker".into()),
        client_secret: Some(OauthClientSecretRef {
            secret_name: "kc-introspection-secret".into(),
            key: "secret".into(),
        }),
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

    let kafka = kafka_cr_with_listeners(
        "c15",
        "ns15",
        vec![
            oauth_listener("oauth-a", 9095, true, cfg_a),
            oauth_listener("oauth-b", 9096, true, cfg_b),
        ],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(&observed, "c15", "ConflictingOAuthConfig");
}

// ── test 16: maxSecondsWithoutReauthentication threads through to broker TOML ─

/// An OAuth listener with
/// `maxSecondsWithoutReauthentication: 300` reconciles successfully and
/// the rendered broker-config `ConfigMap` embeds the broker-global
/// `max_session_lifetime_seconds = 300` line under `[oauthbearer]`. The
/// broker uses this as a ceiling on `session_lifetime_ms` returned to
/// SASL clients; the dispatch-loop KIP-368 timer fires at the clamped
/// time.
#[tokio::test]
async fn oauth_listener_with_max_seconds_without_reauthentication_renders_broker_toml_key() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns16", "c16", 1, 1)];
    let (ctx, state) = build_ctx("ns16", happy_path_rules("c16", "ns16", &items));

    let mut cfg = oauth_cfg_minimal();
    cfg.max_seconds_without_reauthentication = Some(300);
    let kafka = kafka_cr_with_listeners(
        "c16",
        "ns16",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c16");

    assert!(toml.contains("[oauthbearer]"), "TOML: {toml}");
    assert!(
        toml.contains("max_session_lifetime_seconds = 300"),
        "expected rendered broker TOML to include max_session_lifetime_seconds = 300;\nTOML: {toml}"
    );
}

// ── test 17: divergent maxSecondsWithoutReauthentication → ConflictingOAuthConfig

/// Two OAuth listeners that each pass per-listener validation
/// but disagree on `maxSecondsWithoutReauthentication` (one capped at
/// 300s, the other at 600s) cannot share the broker-global
/// `[oauthbearer]` block — the cap is part of the canonical fingerprint
/// (T3's divergence-walk perturbation list). The reconciler must reject
/// with `ListenersValid=False` reason `ConflictingOAuthConfig`.
#[tokio::test]
async fn two_oauth_listeners_with_divergent_max_seconds_without_reauthentication_rejected_with_conflicting_oauth_config()
 {
    let items = vec![shared::fake_pool_list_item("brokers", "ns17", "c17", 1, 1)];
    let rules = rules_for_invalid_listeners("c17", "ns17", &items);
    let (ctx, state) = build_ctx("ns17", rules);

    let mut cfg_a = oauth_cfg_minimal();
    cfg_a.max_seconds_without_reauthentication = Some(300);
    let mut cfg_b = oauth_cfg_minimal();
    cfg_b.max_seconds_without_reauthentication = Some(600);
    let kafka = kafka_cr_with_listeners(
        "c17",
        "ns17",
        vec![
            oauth_listener("oauth-a", 9095, true, cfg_a),
            oauth_listener("oauth-b", 9096, true, cfg_b),
        ],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(&observed, "c17", "ConflictingOAuthConfig");
}

// ── customClaimCheck JsonPath renders to broker TOML ──────────────────────

/// An OAuth listener with a `customClaimCheck` `JsonPath`
/// expression (RFC 9535 syntax, evaluated by jsonpath-rust on the
/// broker) reconciles cleanly and the rendered broker-config `ConfigMap`
/// embeds the expression under `[oauthbearer].custom_claim_check` as a
/// TOML multi-line literal string (triple-single-quoted so no escape
/// processing collides with the `'` chars inside the path predicate).
#[tokio::test]
async fn oauth_listener_with_custom_claim_check_expression_renders_broker_toml_key() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns18", "c18", 1, 1)];
    let (ctx, state) = build_ctx("ns18", happy_path_rules("c18", "ns18", &items));

    let mut cfg = oauth_cfg_minimal();
    cfg.custom_claim_check = Some("$.scope[?@ == 'kafka.write']".into());
    let kafka = kafka_cr_with_listeners(
        "c18",
        "ns18",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c18");

    assert!(toml.contains("[oauthbearer]"), "TOML: {toml}");
    assert!(
        toml.contains("custom_claim_check = '''$.scope[?@ == 'kafka.write']'''"),
        "expected custom_claim_check render; got:\n{toml}"
    );
}

// ── validTokenType renders to broker TOML ─────────────────────────────────

/// An OAuth listener with `validTokenType: JWT` reconciles
/// cleanly (JWT-mode is the only mode that accepts the field) and the
/// rendered broker-config `ConfigMap` embeds the value under
/// `[oauthbearer].valid_token_type` as a basic TOML string. The broker
/// JWT validators enforce the `typ` header check at token-verify time.
#[tokio::test]
async fn oauth_listener_with_valid_token_type_renders_broker_toml_key() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns19", "c19", 1, 1)];
    let (ctx, state) = build_ctx("ns19", happy_path_rules("c19", "ns19", &items));

    let mut cfg = oauth_cfg_minimal();
    cfg.valid_token_type = Some("JWT".into());
    let kafka = kafka_cr_with_listeners(
        "c19",
        "ns19",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c19");

    assert!(toml.contains("[oauthbearer]"), "TOML: {toml}");
    assert!(
        toml.contains("valid_token_type = \"JWT\""),
        "expected valid_token_type render; got:\n{toml}"
    );
}

// ── validTokenType in introspection mode → reject ─────────────────────────

/// Setting `validTokenType` on an introspection-mode listener
/// (`accessTokenIsJwt: false`) is rejected up front: introspection
/// responses carry no JWT header so a `typ` check has nothing to bind
/// against. The reconciler must patch `ListenersValid=False` with reason
/// `ListenerOauthValidTokenTypeRejectedInIntrospectionMode`. Mirrors the
/// `validate_listeners_rejects_valid_token_type_in_introspection_mode`
/// unit test at the integration layer.
#[tokio::test]
async fn oauth_listener_valid_token_type_in_introspection_mode_rejected_with_listeners_valid_false()
{
    let items = vec![shared::fake_pool_list_item("brokers", "ns20", "c20", 1, 1)];
    let rules = rules_for_invalid_listeners("c20", "ns20", &items);
    let (ctx, state) = build_ctx("ns20", rules);

    let mut cfg = oauth_cfg_minimal();
    cfg.access_token_is_jwt = false;
    cfg.jwks_endpoint_uri = None;
    cfg.introspection_endpoint_uri = Some("https://iss.example/introspect".into());
    cfg.client_id = Some("kafka-broker".into());
    cfg.client_secret = Some(OauthClientSecretRef {
        secret_name: "creds".into(),
        key: "client-secret".into(),
    });
    cfg.valid_token_type = Some("JWT".into()); // the violation

    let kafka = kafka_cr_with_listeners(
        "c20",
        "ns20",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(
        &observed,
        "c20",
        "ListenerOauthValidTokenTypeRejectedInIntrospectionMode",
    );
}

// ── fallbackUserNameClaim + prefix render to broker TOML ──────────────────

/// An OAuth listener with `fallbackUserNameClaim: "client_id"`
/// and `fallbackUserNamePrefix: "service-account-"` reconciles cleanly
/// and the rendered broker-config `ConfigMap` embeds both keys under
/// `[oauthbearer]`. The broker's principal extractor consults the
/// fallback claim only when `userNameClaim` (default `sub`) is
/// absent/empty on the incoming token, then prepends the prefix to the
/// resolved name. Strimzi convention for Keycloak service-account
/// tokens whose `sub` is a UUID.
#[tokio::test]
async fn oauth_listener_with_fallback_user_name_claim_renders_broker_toml_key() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns21", "c21", 1, 1)];
    let (ctx, state) = build_ctx("ns21", happy_path_rules("c21", "ns21", &items));

    let mut cfg = oauth_cfg_minimal();
    cfg.fallback_user_name_claim = Some("client_id".into());
    cfg.fallback_user_name_prefix = Some("service-account-".into());
    let kafka = kafka_cr_with_listeners(
        "c21",
        "ns21",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c21");

    for needle in [
        "[oauthbearer]",
        "fallback_user_name_claim = \"client_id\"",
        "fallback_user_name_prefix = \"service-account-\"",
    ] {
        assert!(toml.contains(needle), "missing {needle:?} in TOML:\n{toml}");
    }
}

// ── groupsClaim JsonPath + delimiter render to broker TOML ────────────────

/// An OAuth listener with `groupsClaim:
/// "$.realm_access.roles[*]"` (RFC 9535 `JsonPath`, evaluated by
/// jsonpath-rust on the broker) and `groupsClaimDelimiter: ","`
/// reconciles cleanly and the rendered broker-config `ConfigMap` embeds
/// both keys under `[oauthbearer]`. The path is emitted as a TOML
/// multi-line literal string (triple-single-quoted) so the `[*]`
/// selector and any future predicate single-quotes survive without
/// escape collisions. The delimiter is a plain TOML basic string. The
/// resolved groups are attached to the Kafka principal but no
/// broker-side authorizer reads them yet.
#[tokio::test]
async fn oauth_listener_with_groups_claim_renders_broker_toml_key() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns22", "c22", 1, 1)];
    let (ctx, state) = build_ctx("ns22", happy_path_rules("c22", "ns22", &items));

    let mut cfg = oauth_cfg_minimal();
    cfg.groups_claim = Some("$.realm_access.roles[*]".into());
    cfg.groups_claim_delimiter = Some(",".into());
    let kafka = kafka_cr_with_listeners(
        "c22",
        "ns22",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c22");

    for needle in [
        "[oauthbearer]",
        "groups_claim = '''$.realm_access.roles[*]'''",
        "groups_claim_delimiter = \",\"",
    ] {
        assert!(toml.contains(needle), "missing {needle:?} in TOML:\n{toml}");
    }
}

// ── JWKS refresher policy fields render to broker TOML ────────────────────

/// An OAuth listener with `jwksMinRefreshPauseSeconds: 2`,
/// `jwksExpirySeconds: 3600`, and `jwksIgnoreKeyUse: true` reconciles
/// cleanly and the rendered broker-config `ConfigMap` embeds all three
/// keys under `[oauthbearer]`. The broker's JWKS refresher consumes
/// them: `min_on_demand_pause` rate-limits on-demand refreshes,
/// `expiry_ms` is the hard fail-closed cache age the signed-JWT
/// validator pre-checks against `last_successful_fetch`, and
/// `ignore_key_use` toggles whether `use=enc` JWK entries are filtered
/// out at parse time.
#[tokio::test]
async fn oauth_listener_with_jwks_policies_renders_broker_toml_keys() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns23", "c23", 1, 1)];
    let (ctx, state) = build_ctx("ns23", happy_path_rules("c23", "ns23", &items));

    let mut cfg = oauth_cfg_minimal();
    cfg.jwks_min_refresh_pause_seconds = Some(2);
    cfg.jwks_expiry_seconds = Some(3600);
    cfg.jwks_ignore_key_use = Some(true);
    let kafka = kafka_cr_with_listeners(
        "c23",
        "ns23",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c23");

    for needle in [
        "[oauthbearer]",
        "jwks_min_refresh_pause_seconds = 2",
        "jwks_expiry_seconds = 3600",
        "jwks_ignore_key_use = true",
    ] {
        assert!(toml.contains(needle), "missing {needle:?} in TOML:\n{toml}");
    }
}

// ── JWKS policy fields rejected on introspection-mode ─────────────────────

/// The 3 JWKS refresher policy fields
/// (`jwksMinRefreshPauseSeconds`, `jwksExpirySeconds`,
/// `jwksIgnoreKeyUse`) are JWT-mode only — the broker's introspection
/// validator does not consult a JWKS, so setting any of them on an
/// `accessTokenIsJwt: false` listener is rejected at reconcile with
/// `ListenersValid=False` reason
/// `ListenerOauthJwksFieldsRejectedInIntrospectionMode`. Mirrors the
/// `validTokenType` and other cross-mode rejection shapes.
#[tokio::test]
async fn oauth_listener_jwks_fields_in_introspection_mode_rejected_with_listeners_valid_false() {
    let items = vec![shared::fake_pool_list_item("brokers", "ns24", "c24", 1, 1)];
    let rules = rules_for_invalid_listeners("c24", "ns24", &items);
    let (ctx, state) = build_ctx("ns24", rules);

    let mut cfg = oauth_cfg_minimal();
    cfg.access_token_is_jwt = false;
    cfg.jwks_endpoint_uri = None;
    cfg.introspection_endpoint_uri = Some("https://iss.example/introspect".into());
    cfg.client_id = Some("kafka-broker".into());
    cfg.client_secret = Some(OauthClientSecretRef {
        secret_name: "creds".into(),
        key: "client-secret".into(),
    });
    cfg.jwks_expiry_seconds = Some(3600); // the violation

    let kafka = kafka_cr_with_listeners(
        "c24",
        "ns24",
        vec![oauth_listener("oauth", 9095, true, cfg)],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_listeners_invalid_with_reason(
        &observed,
        "c24",
        "ListenerOauthJwksFieldsRejectedInIntrospectionMode",
    );
}
