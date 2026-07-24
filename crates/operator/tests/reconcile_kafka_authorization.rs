//! Integration tests for `Kafka.spec.authorization` —
//! verifies the operator reconcile renders the `[authorization]` TOML
//! block into the broker `ConfigMap` for both `simple` and `opa`
//! variants.
//!
//! The pure-function render tests already live alongside
//! `render_broker_toml` in `controller/listeners.rs`. The added value at
//! this layer is verifying that the rendered TOML actually lands in the
//! broker-config `ConfigMap` PATCH (`<cluster>-broker-config`, data key
//! `broker-<id>.toml`) on a real reconcile.

use std::sync::Arc;

use assert2::assert;
use crabka_operator::{
    controller::kafka::reconcile,
    crd::{Authorization, Kafka, KafkaSpec, OpaAuthorization, SimpleAuthorization},
};
use http::Method;

#[path = "shared/mod.rs"]
mod shared;

use shared::{build_ctx, fake_pool_list_item, happy_path_rules};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a `Kafka` CR with the given namespace, name, and authorization
/// spec. Mirrors the helper shape used in `reconcile_inter_broker_mtls.rs`
/// + `reconcile_listener_oauth.rs`.
fn kafka_cr_with_authorization(
    name: &str,
    namespace: &str,
    authorization: Option<Authorization>,
) -> Kafka {
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
            authorization,
            tiered_storage: None,
            inter_broker_kerberos: None,
            krb5_conf_secret_ref: None,
            tracing: None,
            broker_tuning: None,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

/// Extract the `broker-0.toml` string from the captured `ConfigMap` PATCH
/// body. Panics with a helpful message when the PATCH (or the key) is
/// missing — both are reconcile-path invariants for these tests.
fn broker_0_toml_from_observed(
    observed: &[http::Request<hyper::body::Bytes>],
    cluster: &str,
) -> String {
    let cm_uri = format!("/configmaps/{cluster}-broker-config");
    let cm_req = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains(&cm_uri))
        .unwrap_or_else(|| panic!("ConfigMap PATCH at {cm_uri} must have been captured"));
    let body: serde_json::Value =
        serde_json::from_slice(cm_req.body()).expect("ConfigMap PATCH body is JSON");
    body["data"]["broker-0.toml"]
        .as_str()
        .unwrap_or_else(|| panic!("broker-0.toml key missing from CM PATCH; body = {body}"))
        .to_string()
}

// ── test 1: type: opa renders [authorization] + [authorization.opa] ──────────

/// A `Kafka` spec with `authorization: { type: opa,
/// url, superUsers: ["ANONYMOUS"] }` must produce a broker `ConfigMap`
/// whose `broker-0.toml` data field carries the `[authorization]` block
/// with `type = "opa"`, `super_users = ["ANONYMOUS"]`, and a nested
/// `[authorization.opa]` table with the configured `url`.
#[tokio::test]
async fn kafka_with_opa_authorization_renders_correct_broker_toml() {
    let items = vec![fake_pool_list_item("brokers", "y", "c1", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("c1", "y", &items));

    let kafka = kafka_cr_with_authorization(
        "c1",
        "y",
        Some(Authorization::Opa(OpaAuthorization {
            url: "http://opa:8181/v1/data/k/a".into(),
            super_users: vec!["ANONYMOUS".into()],
            allow_on_error: None,
            initial_cache_capacity: None,
            maximum_cache_size: None,
            expire_after_ms: None,
        })),
    );

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml_str = broker_0_toml_from_observed(&observed, "c1");

    for needle in [
        "[authorization]",
        "type = \"opa\"",
        "super_users = [\"ANONYMOUS\"]",
        "[authorization.opa]",
        "url = \"http://opa:8181/v1/data/k/a\"",
    ] {
        assert!(toml_str.contains(needle), "{needle} missing;\n{toml_str}");
    }

    // Round-trip parse through the broker's own FileConfig — sanity check
    // that the rendered TOML is structurally valid (matches the broker's
    // `deny_unknown_fields` schema).
    let parsed: crabka_broker::file_config::FileConfig =
        toml::from_str(&toml_str).expect("broker-0.toml must parse as FileConfig");
    let a = parsed
        .authorization
        .expect("FileConfig.authorization must be Some when [authorization] is rendered");
    let opa = a
        .opa
        .expect("FileConfig.authorization.opa must be Some for type = \"opa\"");
    assert!(opa.url == "http://opa:8181/v1/data/k/a");
    assert!(a.super_users == vec!["ANONYMOUS".to_string()]);
}

// ── test 2: type: simple round-trips super_users ─────────────────────────────

/// A `Kafka` spec with `authorization: { type:
/// simple, superUsers: ["User:admin"] }` must produce a broker
/// `ConfigMap` whose `broker-0.toml` carries `[authorization]` with
/// `type = "simple"` and `super_users = ["User:admin"]`. No
/// `[authorization.opa]` subtable should appear.
#[tokio::test]
async fn kafka_with_simple_authorization_super_users_round_trip() {
    let items = vec![fake_pool_list_item("brokers", "y", "c1", 1, 1)];
    let (ctx, state) = build_ctx("y", happy_path_rules("c1", "y", &items));

    let kafka = kafka_cr_with_authorization(
        "c1",
        "y",
        Some(Authorization::Simple(SimpleAuthorization {
            super_users: vec!["User:admin".into()],
        })),
    );

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml_str = broker_0_toml_from_observed(&observed, "c1");

    // `[authorization.opa]` must NOT appear for type = "simple".
    for (needle, want) in [
        ("[authorization]", true),
        ("type = \"simple\"", true),
        ("super_users = [\"User:admin\"]", true),
        ("[authorization.opa]", false),
    ] {
        assert!(
            toml_str.contains(needle) == want,
            "{needle}: expected present={want} for type = \"simple\";\n{toml_str}"
        );
    }

    // Round-trip parse for structural validity.
    let parsed: crabka_broker::file_config::FileConfig =
        toml::from_str(&toml_str).expect("broker-0.toml must parse as FileConfig");
    let a = parsed
        .authorization
        .expect("FileConfig.authorization must be Some when [authorization] is rendered");
    assert!(
        a.opa.is_none(),
        "FileConfig.authorization.opa must be None for type = \"simple\""
    );
    assert!(a.super_users == vec!["User:admin".to_string()]);
}
