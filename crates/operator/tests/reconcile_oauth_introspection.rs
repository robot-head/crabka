#![allow(clippy::doc_markdown, clippy::doc_lazy_continuation)]
//! Integration tests for the operator's OAUTHBEARER
//! introspection-secret surface. Verifies the full reconcile path:
//! source-Secret validation (presence / key / value), the
//! short-circuit when the listener is in JWT mode, the rendered
//! `[oauthbearer]` TOML block in introspection mode (including
//! `userinfo_endpoint_uri` when set), and the StatefulSet pod-template
//! mount of the user-owned source Secret with a projected `items`
//! mapping that pins the user's key to the fixed in-pod filename
//! `client-secret`.
//!
//! T5's unit tests in `controller/kafka_node_pool.rs` cover the pod-
//! template render in isolation. T6's integration tests exercise the
//! validate-source-Secret code paths and the status-condition wiring
//! end-to-end, plus the integration-level pod-template mount via the
//! pool reconciler.

use std::sync::Arc;

use base64::Engine as _;
use crabka_operator::{
    controller::{
        kafka::reconcile as reconcile_kafka, kafka_node_pool::reconcile as reconcile_pool,
    },
    crd::{
        Kafka, KafkaSpec, Listener, ListenerAuthentication, ListenerAuthenticationOAuth,
        ListenerType, OauthClientSecretRef,
    },
};
use http::Method;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    assert_ready_false_with_reason, build_ctx, extract_broker0_toml, fake_pool_list_item,
    happy_path_rules, pool_cr, pool_reconcile_rules, rule_get_secret, rule_get_secret_404,
    rules_for_failure_path,
};

// ── fixtures ────────────────────────────────────────────────────────────────

const SOURCE_SECRET_NAME: &str = "keycloak-introspection-secret";
const SOURCE_KEY: &str = "secret";
const SOURCE_VALUE: &[u8] = b"shhh-broker-secret";

const INTROSPECTION_URI: &str =
    "https://keycloak.example/realms/kafka/protocol/openid-connect/token/introspect";
const USERINFO_URI: &str = "https://keycloak.example/realms/kafka/protocol/openid-connect/userinfo";
const CLIENT_ID: &str = "kafka-broker";
const VALID_AUDIENCE: &str = "kafka-broker";
const USER_NAME_CLAIM: &str = "preferred_username";

/// Build a `ListenerAuthenticationOAuth` in introspection mode. The
/// `client_secret` points at `(SOURCE_SECRET_NAME, SOURCE_KEY)` by
/// default; tests override only what they need.
fn introspection_oauth_cfg() -> ListenerAuthenticationOAuth {
    ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://keycloak.example/realms/kafka".into(),
        jwks_endpoint_uri: None,
        valid_audience: Some(VALID_AUDIENCE.into()),
        user_name_claim: Some(USER_NAME_CLAIM.into()),
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
        access_token_is_jwt: false,
        introspection_endpoint_uri: Some(INTROSPECTION_URI.into()),
        user_info_endpoint_uri: None,
        client_id: Some(CLIENT_ID.into()),
        client_secret: Some(OauthClientSecretRef {
            secret_name: SOURCE_SECRET_NAME.into(),
            key: SOURCE_KEY.into(),
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
    }
}

fn oauth_listener(name: &str, port: i32, cfg: ListenerAuthenticationOAuth) -> Listener {
    Listener {
        name: name.into(),
        port,
        type_: ListenerType::Internal,
        tls: true,
        authentication: Some(ListenerAuthentication::OAuth(cfg)),
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

/// JSON body shaped like a `core/v1/Secret` with one base64-encoded
/// data key. Used as the GET response for the introspection source-
/// Secret read.
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
/// used to exercise the `MissingOauthIntrospectionKey` branch (Secret
/// exists but lacks the named key).
fn source_secret_body_no_data(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": format!("{name}-uid") },
        "type": "Opaque",
    })
}

// ── test 1: happy path — source Secret read, mount derived ──────────────────

/// An introspection-mode OAuth listener with a `clientSecret` ref to
/// an existing Secret reconciles cleanly: the source Secret is GET'd,
/// validated, and the reconcile proceeds past the introspection guard
/// into per-broker rendering. Asserts that the source-Secret GET fires
/// against the expected URI and that the reconcile reaches the
/// ConfigMap PATCH step (proving we made it past validation).
#[tokio::test]
async fn oauth_introspection_validates_source_secret_and_mounts_it() {
    let items = vec![fake_pool_list_item("brokers", "ns1", "c1", 1, 1)];
    let mut rules = happy_path_rules("c1", "ns1", &items);
    // Source-Secret GET fires after the trust-bundle assembly step (a
    // no-op for this fixture: no `tls_trusted_certificates`) and before
    // the per-broker keystore step. FIFO substring matching means the
    // ordering among non-overlapping substrings doesn't matter.
    rules.push(rule_get_secret(
        SOURCE_SECRET_NAME,
        &source_secret_body(SOURCE_SECRET_NAME, "ns1", SOURCE_KEY, SOURCE_VALUE),
    ));

    let (ctx, state) = build_ctx("ns1", rules);
    let kafka = kafka_cr(
        "c1",
        "ns1",
        vec![oauth_listener("oauth", 9095, introspection_oauth_cfg())],
    );
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // Source-Secret GET fired.
    assert2::assert!(observed.iter().any(|r| {
        r.method() == Method::GET
            && r.uri()
                .to_string()
                .contains(&format!("/secrets/{SOURCE_SECRET_NAME}"))
    }));

    // Reconcile made it past validation to the per-broker ConfigMap.
    let _ = extract_broker0_toml(&observed, "c1");
}

// ── test 2: missing source Secret → MissingOauthIntrospectionSecret ─────────

/// Source Secret entirely absent (mock returns 404 on the `get_opt`).
/// After reconcile, the Kafka CR status PATCH carries a `Ready`
/// condition with `status: "False"` and `reason:
/// "MissingOauthIntrospectionSecret"`.
#[tokio::test]
async fn oauth_introspection_missing_source_secret_rejects_with_missing_oauth_introspection_secret()
{
    let mut rules = rules_for_failure_path("c2", "ns2");
    rules.push(rule_get_secret_404(SOURCE_SECRET_NAME));

    let (ctx, state) = build_ctx("ns2", rules);
    let kafka = kafka_cr(
        "c2",
        "ns2",
        vec![oauth_listener("oauth", 9095, introspection_oauth_cfg())],
    );
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    assert_ready_false_with_reason(&observed, "c2", "MissingOauthIntrospectionSecret");

    // No ConfigMap PATCH on a failure path.
    assert2::assert!(!observed.iter().any(|r| r.method() == Method::PATCH
        && r.uri().to_string().contains("/configmaps/c2-broker-config")));
}

// ── test 3: Secret present but key absent → MissingOauthIntrospectionKey ────

/// Secret exists but lacks the named key. Reason:
/// `MissingOauthIntrospectionKey`.
#[tokio::test]
async fn oauth_introspection_invalid_secret_value_cases() {
    for (name, cluster, namespace, secret, reason) in [
        (
            "missing key",
            "c3",
            "ns3",
            source_secret_body_no_data(SOURCE_SECRET_NAME, "ns3"),
            "MissingOauthIntrospectionKey",
        ),
        (
            "empty value",
            "c4",
            "ns4",
            source_secret_body(SOURCE_SECRET_NAME, "ns4", SOURCE_KEY, b""),
            "EmptyOauthIntrospectionValue",
        ),
    ] {
        let mut rules = rules_for_failure_path(cluster, namespace);
        rules.push(rule_get_secret(SOURCE_SECRET_NAME, &secret));
        let (ctx, state) = build_ctx(namespace, rules);
        let kafka = kafka_cr(
            cluster,
            namespace,
            vec![oauth_listener("oauth", 9095, introspection_oauth_cfg())],
        );
        reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();
        assert_ready_false_with_reason(&state.take_observed(), cluster, reason);
        let _ = name;
    }
}

// ── test 5: JWT-mode short-circuits — no source-Secret read, no mount ───────

/// A JWT-mode OAuth listener (`accessTokenIsJwt: true`) must NOT cause
/// the introspection-Secret validator to fire (it short-circuits to
/// `Ok(None)`), and the rendered StatefulSet (via the pool reconciler)
/// must NOT carry an `oauth-introspection-secret` volume.
///
/// This test runs only the pool reconciler — the cluster reconciler's
/// fixture has no apiserver responder for a `GET /secrets/...` against
/// a non-existent introspection source, so re-using `happy_path_rules`
/// for the cluster pass would always 404-with-unmatched-rule. The pool
/// reconciler is what mounts the volume, so testing the mount-absence
/// at that level is the right contract.
#[tokio::test]
async fn oauth_introspection_jwt_mode_does_not_mount_anything() {
    let rules = pool_reconcile_rules(
        "c5",
        "brokers",
        "ns5",
        &parent_kafka_body_with_oauth(
            "c5", "ns5", /* jwt_mode = */ true, /* userinfo = */ None,
        ),
    );
    let (ctx, state) = build_ctx("ns5", rules);
    let pool = pool_cr("brokers", "ns5", "c5", 1);
    reconcile_pool(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/statefulsets/c5-brokers")
        })
        .expect("StatefulSet PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS body is JSON");
    let pod_spec = &body["spec"]["template"]["spec"];

    let volumes = pod_spec["volumes"].as_array().expect("volumes array");
    assert2::assert!(
        volumes
            .iter()
            .all(|v| v["name"] != "oauth-introspection-secret")
    );

    let containers = pod_spec["containers"].as_array().expect("containers array");
    let broker = containers
        .iter()
        .find(|c| c["name"] == "broker")
        .unwrap_or_else(|| panic!("broker container present; body = {body}"));
    let mounts = broker["volumeMounts"].as_array().expect("volumeMounts");
    assert2::assert!(
        mounts
            .iter()
            .all(|m| m["name"] != "oauth-introspection-secret")
    );
}

// ── test 6: managed pod template mounts secret with projected items ─────────

/// Introspection-mode pool reconcile renders a pod template with the
/// user's source Secret as an `oauth-introspection-secret` volume.
/// The projected `items[0]` mapping pins the user's key
/// (`SOURCE_KEY` = "secret") to the fixed in-pod filename
/// `client-secret` — which is the path the broker's rendered TOML
/// references (`/etc/crabka/oauth-introspection/client-secret`).
#[tokio::test]
async fn oauth_introspection_managed_pod_template_mounts_secret_with_projected_items() {
    let rules = pool_reconcile_rules(
        "c6",
        "brokers",
        "ns6",
        &parent_kafka_body_with_oauth(
            "c6", "ns6", /* jwt_mode = */ false, /* userinfo = */ None,
        ),
    );
    let (ctx, state) = build_ctx("ns6", rules);
    let pool = pool_cr("brokers", "ns6", "c6", 1);
    reconcile_pool(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/statefulsets/c6-brokers")
        })
        .expect("StatefulSet PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS body is JSON");
    let pod_spec = &body["spec"]["template"]["spec"];

    let volumes = pod_spec["volumes"].as_array().expect("volumes array");
    let intro_vol = volumes
        .iter()
        .find(|v| v["name"] == "oauth-introspection-secret")
        .unwrap_or_else(|| panic!("oauth-introspection-secret volume present; body = {body}"));
    assert2::assert!(
        intro_vol["secret"]
            == serde_json::json!({
                "defaultMode": 256,
                "secretName": SOURCE_SECRET_NAME,
                "items": [{ "key": SOURCE_KEY, "path": "client-secret" }],
            })
    );
}

// ── test 7: userinfo endpoint renders into [oauthbearer] TOML ──────────────

/// When the introspection-mode OAuth listener has
/// `userInfoEndpointUri` set, the rendered broker `[oauthbearer]` TOML
/// block must contain `userinfo_endpoint_uri = "..."` (T3's render
/// path). Verified at integration scope by reading the captured
/// broker-config ConfigMap PATCH body from the cluster reconciler.
#[tokio::test]
async fn oauth_introspection_with_userinfo_renders_userinfo_endpoint_in_toml() {
    let items = vec![fake_pool_list_item("brokers", "ns7", "c7", 1, 1)];
    let mut rules = happy_path_rules("c7", "ns7", &items);
    rules.push(rule_get_secret(
        SOURCE_SECRET_NAME,
        &source_secret_body(SOURCE_SECRET_NAME, "ns7", SOURCE_KEY, SOURCE_VALUE),
    ));

    let (ctx, state) = build_ctx("ns7", rules);
    let mut cfg = introspection_oauth_cfg();
    cfg.user_info_endpoint_uri = Some(USERINFO_URI.into());
    let kafka = kafka_cr("c7", "ns7", vec![oauth_listener("oauth", 9095, cfg)]);
    reconcile_kafka(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c7");

    // With userInfoEndpointUri set, the [oauthbearer] block must carry the
    // userinfo endpoint AND still carry the introspection endpoint.
    for needle in [
        "[oauthbearer]".to_string(),
        format!("userinfo_endpoint_uri = \"{USERINFO_URI}\""),
        format!("introspection_endpoint_uri = \"{INTROSPECTION_URI}\""),
    ] {
        assert2::assert!(toml.contains(&needle));
    }
}

// ── test 8: StatefulSet mounts the introspection Secret end-to-end ─────────

/// End-to-end mount assertion at integration scope (analogous to T5's
/// unit test but driven through the full pool reconcile pipeline).
/// Confirms the FIFO mock surface for the pool's parent-Kafka GET is
/// enough to drive the introspection-mode mount derivation.
#[tokio::test]
async fn statefulset_mounts_oauth_introspection_secret_when_introspection_mode() {
    let rules = pool_reconcile_rules(
        "c8",
        "brokers",
        "ns8",
        &parent_kafka_body_with_oauth(
            "c8", "ns8", /* jwt_mode = */ false, /* userinfo = */ None,
        ),
    );
    let (ctx, state) = build_ctx("ns8", rules);
    let pool = pool_cr("brokers", "ns8", "c8", 1);
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

    // Volume entry sources the user's Secret directly.
    let volumes = pod_spec["volumes"].as_array().expect("volumes array");
    let intro_vol = volumes
        .iter()
        .find(|v| v["name"] == "oauth-introspection-secret")
        .unwrap_or_else(|| panic!("oauth-introspection-secret volume present; body = {body}"));
    assert2::assert!(
        intro_vol["secret"]
            == serde_json::json!({
                "defaultMode": 256,
                "secretName": SOURCE_SECRET_NAME,
                "items": [{ "key": SOURCE_KEY, "path": "client-secret" }],
            })
    );

    // VolumeMount on the broker container at the canonical path.
    let containers = pod_spec["containers"].as_array().expect("containers array");
    let broker = containers
        .iter()
        .find(|c| c["name"] == "broker")
        .unwrap_or_else(|| panic!("broker container present; body = {body}"));
    let mounts = broker["volumeMounts"].as_array().expect("volumeMounts");
    let intro_mount = mounts
        .iter()
        .find(|m| m["name"] == "oauth-introspection-secret")
        .unwrap_or_else(|| panic!("oauth-introspection-secret mount present; body = {body}"));
    assert2::assert!(intro_mount["mountPath"].as_str() == Some("/etc/crabka/oauth-introspection"));
    assert2::assert!(intro_mount["readOnly"].as_bool() == Some(true));
}

// ── test 9: StatefulSet omits introspection volume when JWT mode ───────────

// Symmetric absence assertion: a JWT-mode parent Kafka produces a
// StatefulSet pod template with no `oauth-introspection-secret`
// volume or mount. Mirrors
// `statefulset_omits_oauth_jwks_trust_volume_when_no_trust_certs`
// shape.
// ── pool-reconcile fixtures (tests 5, 6, 8, 9) ─────────────────────────────

// Parent-Kafka body that carries an OAuth listener in either JWT or
// introspection mode (per `jwt_mode`). Used as the GET response for the pool
// reconciler's `kafka_api.get_opt(parent_name)` step so the rendered pod
// template picks up the right branch of `oauth_introspection_secret_mount`.
fn parent_kafka_body_with_oauth(
    name: &str,
    namespace: &str,
    jwt_mode: bool,
    userinfo: Option<&str>,
) -> serde_json::Value {
    let authentication = if jwt_mode {
        serde_json::json!({
            "type": "oauth",
            "validIssuerUri": "https://keycloak.example/realms/kafka",
            "jwksEndpointUri":
                "https://keycloak.example/realms/kafka/protocol/openid-connect/certs",
            "accessTokenIsJwt": true,
        })
    } else {
        let mut auth = serde_json::json!({
            "type": "oauth",
            "validIssuerUri": "https://keycloak.example/realms/kafka",
            "accessTokenIsJwt": false,
            "introspectionEndpointUri": INTROSPECTION_URI,
            "clientId": CLIENT_ID,
            "clientSecret": {
                "secretName": SOURCE_SECRET_NAME,
                "key": SOURCE_KEY,
            },
            "validAudience": VALID_AUDIENCE,
            "userNameClaim": USER_NAME_CLAIM,
        });
        if let Some(uri) = userinfo {
            auth["userInfoEndpointUri"] = serde_json::Value::String(uri.into());
        }
        auth
    };
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
                "authentication": authentication,
            }]
        },
        // A reconciled parent carries a cleared version model; the pool
        // reconciler gates pod creation on it (see
        // `kafka_node_pool::version_gate`).
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
