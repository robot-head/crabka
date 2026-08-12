//! Reconcile-level tests for the `KafkaUser` controller.

use std::{collections::BTreeMap, sync::Arc};

use assert2::{assert, check};
use crabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, PatternType, PermissionType, QuotaOp, ResourceType,
    UserQuotaConfig,
};
use crabka_operator::{
    controller::user::reconcile,
    crd::{
        AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule,
        Authentication, KafkaUser, KafkaUserAuthorization as Authorization, KafkaUserQuotas,
        KafkaUserSimpleAuthorization as SimpleAuthorization, KafkaUserSpec, ScramSha512Auth,
        user::TlsAuth,
    },
};
use crabka_security::ca;
use http::{Method, Response};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use serde_json::json;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState,
    fake_admin::{FakeAdminClient, RecordedCall},
    fixture_ctx, json_response, mock_client, not_found_body,
};

const CLUSTER: &str = "demo";
const NS: &str = "y";
const USER: &str = "alice";

fn ready_kafka_body(name: &str, namespace: &str) -> serde_json::Value {
    json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": {
            "kafkaVersion": "0.1.1",
            "interBrokerListenerName": "PLAIN",
        },
        "status": {
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "reason": "Available",
                "message": "",
                "lastTransitionTime": "2026-05-17T00:00:00Z",
            }],
            "listeners": [{
                "name": "PLAIN",
                "type": "internal",
                "bootstrapServers": format!(
                    "{name}-broker-headless.{namespace}.svc.cluster.local:9092"
                ),
                "addresses": [],
            }],
        }
    })
}

fn user_body(name: &str, namespace: &str) -> serde_json::Value {
    json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaUser",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": "user-uid",
            "generation": 1,
            "finalizers": ["crabka.io/user-finalizer"],
        },
        "spec": {
            "authentication": {"type": "scram-sha-512"},
        },
        "status": {"conditions": []}
    })
}

fn secret_body(name: &str, namespace: &str, password: &str) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(password.as_bytes());
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "secret-uid" },
        "type": "Opaque",
        "data": { "password": b64 }
    })
}

fn ku_with_finalizer(name: &str, acls: Vec<AclRule>) -> KafkaUser {
    ku_full(name, acls, None)
}

fn ku_full(
    name: &str,
    acls: Vec<AclRule>,
    quotas: Option<crabka_operator::crd::KafkaUserQuotas>,
) -> KafkaUser {
    let mut ku = KafkaUser::new(
        name,
        KafkaUserSpec {
            authentication: Authentication::ScramSha512(ScramSha512Auth::default()),
            authorization: if acls.is_empty() {
                None
            } else {
                Some(Authorization::Simple(SimpleAuthorization { acls }))
            },
            quotas,
        },
    );
    ku.metadata.namespace = Some(NS.into());
    ku.metadata.uid = Some("user-uid".into());
    ku.metadata.generation = Some(1);
    ku.metadata.finalizers = Some(vec!["crabka.io/user-finalizer".into()]);
    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/cluster".into(), CLUSTER.into());
    ku.metadata.labels = Some(labels);
    ku
}

fn rule_topic(name: &str, ops: &[AclOp]) -> AclRule {
    AclRule {
        resource: AclResource {
            kind: AclResourceKind::Topic,
            name: name.into(),
            pattern_type: AclPatternType::Literal,
        },
        operations: ops.to_vec(),
        host: "*".into(),
        permission: AclPermission::Allow,
    }
}

#[tokio::test]
async fn invalid_spec_uses_configured_requeue() {
    let state = MockState::new(vec![MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkausers/{USER}/status"),
        response: json_response(200, &user_body(USER, NS)),
    }]);
    let mut ctx = fixture_ctx(mock_client(&state, NS), NS);
    Arc::get_mut(&mut ctx.config)
        .expect("fixture owns operator config")
        .controller_invalid_requeue = crabka_units::millis(2_345);
    let user = ku_with_finalizer(USER, vec![rule_topic("orders", &[])]);

    let action = reconcile(Arc::new(user), Arc::new(ctx)).await.unwrap();

    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(2_345))
    );
}

#[tokio::test]
async fn missing_cluster_uses_configured_dependency_requeue() {
    let state = MockState::new(vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ]);
    let mut ctx = fixture_ctx(mock_client(&state, NS), NS);
    Arc::get_mut(&mut ctx.config)
        .expect("fixture owns operator config")
        .controller_dependency_requeue = crabka_units::millis(3_456);

    let action = reconcile(Arc::new(ku_with_finalizer(USER, vec![])), Arc::new(ctx))
        .await
        .unwrap();

    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(3_456))
    );
}

#[tokio::test]
async fn secret_read_failure_uses_configured_error_requeue() {
    let state = MockState::new(vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: Response::builder()
                .status(500)
                .header("content-type", "application/json")
                .body(not_found_body("injected failure"))
                .expect("500 builds"),
        },
    ]);
    let mut ctx = fixture_ctx(mock_client(&state, NS), NS);
    Arc::get_mut(&mut ctx.config)
        .expect("fixture owns operator config")
        .controller_error_requeue = crabka_units::millis(4_567);

    let action = reconcile(Arc::new(ku_with_finalizer(USER, vec![])), Arc::new(ctx))
        .await
        .unwrap();

    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(4_567))
    );
}

/// `KafkaUser` with TLS authentication.
fn ku_tls(name: &str, acls: Vec<AclRule>) -> KafkaUser {
    ku_tls_full(name, acls, None, TlsAuth::default())
}

fn ku_tls_full(
    name: &str,
    acls: Vec<AclRule>,
    quotas: Option<KafkaUserQuotas>,
    tls_auth: TlsAuth,
) -> KafkaUser {
    let mut ku = KafkaUser::new(
        name,
        KafkaUserSpec {
            authentication: Authentication::Tls(tls_auth),
            authorization: if acls.is_empty() {
                None
            } else {
                Some(Authorization::Simple(SimpleAuthorization { acls }))
            },
            quotas,
        },
    );
    ku.metadata.namespace = Some(NS.into());
    ku.metadata.uid = Some("user-uid".into());
    ku.metadata.generation = Some(1);
    ku.metadata.finalizers = Some(vec!["crabka.io/user-finalizer".into()]);
    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/cluster".into(), CLUSTER.into());
    ku.metadata.labels = Some(labels);
    ku
}

fn tls_user_secret_body(
    name: &str,
    namespace: &str,
    cert_pem: &str,
    key_pem: &str,
    ca_pem: &str,
) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "user-secret-uid" },
        "type": "Opaque",
        "data": {
            "user.crt": b64.encode(cert_pem.as_bytes()),
            "user.key": b64.encode(key_pem.as_bytes()),
            "ca.crt": b64.encode(ca_pem.as_bytes()),
        }
    })
}

fn clients_ca_key_secret_body(cluster: &str, namespace: &str, key_pem: &str) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": format!("{cluster}-clients-ca"),
            "namespace": namespace,
            "uid": "ca-key-uid",
        },
        "type": "Opaque",
        "data": { "ca.key": b64.encode(key_pem.as_bytes()) }
    })
}

fn clients_ca_cert_secret_body(
    cluster: &str,
    namespace: &str,
    cert_pem: &str,
) -> serde_json::Value {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": format!("{cluster}-clients-ca-cert"),
            "namespace": namespace,
            "uid": "ca-cert-uid",
        },
        "type": "Opaque",
        "data": { "ca.crt": b64.encode(cert_pem.as_bytes()) }
    })
}

/// A `KafkaUser` with the cluster label and no Secret yet. The reconcile
/// creates the Secret, upserts SCRAM, applies the ACLs, and sets
/// Ready=True.
#[tokio::test]
async fn first_reconcile_provisions_scram_and_acls() {
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        // Secret doesn't exist yet.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        // Apply (create) the Secret.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(200, &secret_body(USER, NS, "fake-password")),
        },
        // Final status PATCH.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let mut ctx = fixture_ctx(client, NS);
    Arc::get_mut(&mut ctx.config)
        .expect("fixture owns operator config")
        .controller_drift_requeue = crabka_units::millis(1_234);
    let ctx = Arc::new(ctx);

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_with_finalizer(
        USER,
        vec![rule_topic("orders", &[AclOp::Read, AclOp::Describe])],
    );
    let action = reconcile(Arc::new(ku), ctx).await.unwrap();
    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(1_234))
    );

    let calls = fake_for_assert.lock().await.calls();
    // Expected sequence: AlterUserScramCredentials (upsert) -> DescribeAcls -> CreateAcls.
    assert!(
        calls.iter().any(
            |c| matches!(c, RecordedCall::AlterUserScramCredentials { upsertions, deletions }
                if upsertions.len() == 1 && deletions.is_empty()
                && upsertions[0].username == USER)
        ),
        "expected an AlterUserScramCredentials upsert for {USER}, got {calls:?}",
    );
    assert!(
        calls.iter().any(|c| matches!(c,
            RecordedCall::DescribeAcls(f) if f.principal.as_deref() == Some("User:alice"))),
        "expected a DescribeAcls filtered by principal, got {calls:?}",
    );
    let create = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::CreateAcls(v) => Some(v.clone()),
            _ => None,
        })
        .expect("CreateAcls must have been issued");
    // Two ops fan out into two ACL entries; the BTreeSet diff hands
    // them to CreateAcls in Ord order (Read before Describe).
    assert!(
        create
            == vec![
                AclEntry {
                    resource_type: ResourceType::Topic,
                    resource_name: "orders".into(),
                    pattern_type: PatternType::Literal,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: AclOperation::Read,
                    permission_type: PermissionType::Allow,
                },
                AclEntry {
                    resource_type: ResourceType::Topic,
                    resource_name: "orders".into(),
                    pattern_type: PatternType::Literal,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: AclOperation::Describe,
                    permission_type: PermissionType::Allow,
                },
            ],
        "two ops fan out into two ACL entries",
    );

    // Status patch lands Ready=True.
    let observed = state.take_observed();
    let status = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkausers/alice/status")
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    check!(body["status"]["conditions"][0]["status"] == "True");
    check!(body["status"]["conditions"][0]["reason"] == "Ready");
    check!(body["status"]["username"] == USER);
    check!(body["status"]["secret"] == USER);
    check!(body["status"]["scramSha512"] == true);
}

/// A reconcile with matching ACLs already present makes no `CreateAcls`
/// call and no `DeleteAcls` call.
#[tokio::test]
async fn noop_when_acls_already_match() {
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        // Secret already exists.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(200, &secret_body(USER, NS, "fake-password")),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    // Pre-seed the broker's view with the ACL the spec is going to ask for.
    {
        let mut store = fake.acls.lock().unwrap();
        store.insert(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "orders".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        });
    }
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_with_finalizer(USER, vec![rule_topic("orders", &[AclOp::Read])]);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::CreateAcls(_))),
        "no CreateAcls expected when ACLs already match: {calls:?}",
    );
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::DeleteAcls(_))),
        "no DeleteAcls expected when ACLs already match: {calls:?}",
    );
}

/// The reconcile removes the ACLs that came from outside and are not in
/// the spec. The CRD wins.
#[tokio::test]
async fn deletes_orphan_acls() {
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(200, &secret_body(USER, NS, "fake-password")),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    {
        let mut store = fake.acls.lock().unwrap();
        // Pre-seed an out-of-band ACL the spec doesn't declare.
        store.insert(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "secrets".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        });
    }
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    // An explicit empty authorization block asks the operator to remove all
    // managed ACLs. An absent block leaves broker ACLs untouched.
    let mut ku = ku_with_finalizer(USER, vec![]);
    ku.spec.authorization = Some(Authorization::Simple(SimpleAuthorization { acls: vec![] }));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let delete = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::DeleteAcls(filters) => Some(filters.clone()),
            _ => None,
        })
        .expect("DeleteAcls must have been issued");
    // The reconciler scoped the delete by every axis (no broad filter
    // collapsing into "delete everything for this principal").
    assert!(
        delete
            == vec![AclEntryFilter {
                resource_type: Some(ResourceType::Topic),
                resource_name: Some("secrets".into()),
                pattern_type: Some(PatternType::Literal),
                principal: Some("User:alice".into()),
                host: Some("*".into()),
                operation: Some(AclOperation::Read),
                permission_type: Some(PermissionType::Allow),
            }],
        "delete must be one filter scoped by every axis of the orphan entry",
    );

    // Verify the store is empty after the reconcile completed.
    let store = fake_for_assert.lock().await.acls.lock().unwrap().clone();
    assert!(
        store.is_empty(),
        "delete should have removed every ACL, got: {store:?}",
    );
}

fn quota_rules() -> Vec<MockRule> {
    vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(200, &secret_body(USER, NS, "fake-password")),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ]
}

/// With `spec.quotas` absent, the reconciler never calls Describe and
/// never calls `AlterClientQuotas`. The status carries
/// `quotasInSync: false`.
#[tokio::test]
async fn omitted_quotas_skips_broker_call() {
    let state = MockState::new(quota_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_full(USER, vec![], None);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::DescribeUserQuotas(_))),
        "no DescribeClientQuotas expected when spec.quotas is None: {calls:?}",
    );
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::AlterUserQuotas { .. })),
        "no AlterClientQuotas expected when spec.quotas is None: {calls:?}",
    );

    let observed = state.take_observed();
    let status = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkausers/alice/status")
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["quotasInSync"] == false);
}

/// `spec.quotas` declares per-user limits and the broker has none. The
/// reconciler then issues `AlterClientQuotas` with one `Set` for each
/// declared key.
#[tokio::test]
async fn first_reconcile_sets_declared_quotas() {
    let state = MockState::new(quota_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let quotas = KafkaUserQuotas {
        producer_byte_rate: Some(1_048_576),
        consumer_byte_rate: Some(2_097_152),
        request_percentage: Some(55),
        controller_mutation_rate: Some(10.0),
    };
    let ku = ku_full(USER, vec![], Some(quotas));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let describe = calls
        .iter()
        .find(|c| matches!(c, RecordedCall::DescribeUserQuotas(u) if u == USER));
    assert!(describe.is_some(), "expected DescribeUserQuotas: {calls:?}");

    let alter = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::AlterUserQuotas {
                username,
                ops,
                validate_only,
            } if username == USER => Some((ops.clone(), *validate_only)),
            _ => None,
        })
        .expect("AlterUserQuotas must have been issued");
    let (ops, validate_only) = alter;
    assert!(!validate_only, "production reconcile must commit");
    assert!(
        ops.len() == 4,
        "every set field becomes one Set op: {ops:?}"
    );
    for key in [
        "producer_byte_rate",
        "consumer_byte_rate",
        "request_percentage",
        "controller_mutation_rate",
    ] {
        assert!(
            ops.iter()
                .any(|op| matches!(op, QuotaOp::Set { key: k, .. } if k == key)),
            "missing Set for {key}: {ops:?}",
        );
    }

    // Final status: quotasInSync=true.
    let observed = state.take_observed();
    let status = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkausers/alice/status")
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["quotasInSync"] == true);
}

/// The broker matches the spec exactly, so the reconciler issues no
/// `AlterClientQuotas`. Describe still runs, because it is the input to
/// the diff.
#[tokio::test]
async fn noop_when_quotas_already_match() {
    let state = MockState::new(quota_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    {
        let mut store = fake.user_quotas.lock().unwrap();
        let mut cfg = UserQuotaConfig::new();
        cfg.insert("producer_byte_rate".into(), 1_048_576.0);
        cfg.insert("request_percentage".into(), 25.0);
        store.insert(USER.into(), cfg);
    }
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let quotas = KafkaUserQuotas {
        producer_byte_rate: Some(1_048_576),
        request_percentage: Some(25),
        ..Default::default()
    };
    let ku = ku_full(USER, vec![], Some(quotas));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::AlterUserQuotas { .. })),
        "no AlterClientQuotas expected when quotas already match: {calls:?}",
    );
}

/// The broker has a quota that the spec does not declare. The reconciler
/// then issues a `Remove` for it, because the CRD wins.
#[tokio::test]
async fn drift_remove_path_emits_remove_op() {
    let state = MockState::new(quota_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    {
        let mut store = fake.user_quotas.lock().unwrap();
        let mut cfg = UserQuotaConfig::new();
        cfg.insert("producer_byte_rate".into(), 1.0);
        cfg.insert("consumer_byte_rate".into(), 2.0); // out-of-band
        store.insert(USER.into(), cfg);
    }
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let quotas = KafkaUserQuotas {
        producer_byte_rate: Some(1),
        ..Default::default()
    };
    let ku = ku_full(USER, vec![], Some(quotas));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let ops = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::AlterUserQuotas { ops, .. } => Some(ops.clone()),
            _ => None,
        })
        .expect("AlterUserQuotas must have been issued");
    assert!(ops.len() == 1, "only one drift op expected: {ops:?}");
    assert!(matches!(
        &ops[0],
        QuotaOp::Remove { key } if key == "consumer_byte_rate"
    ));
}

/// `spec.quotas: {}`, an empty object, clears the quota state of this
/// user on the broker. `spec.quotas: null` is different, because it
/// skips the quota work.
#[tokio::test]
async fn empty_quotas_object_tombstones_everything() {
    let state = MockState::new(quota_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    {
        let mut store = fake.user_quotas.lock().unwrap();
        let mut cfg = UserQuotaConfig::new();
        cfg.insert("producer_byte_rate".into(), 1.0);
        cfg.insert("consumer_byte_rate".into(), 2.0);
        store.insert(USER.into(), cfg);
    }
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_full(USER, vec![], Some(KafkaUserQuotas::default()));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let ops = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::AlterUserQuotas { ops, .. } => Some(ops.clone()),
            _ => None,
        })
        .expect("AlterUserQuotas must have been issued");
    assert!(ops.len() == 2, "two existing keys tombstoned: {ops:?}");
    assert!(
        ops.iter().all(|op| matches!(op, QuotaOp::Remove { .. })),
        "every op must be a Remove: {ops:?}",
    );
}

// --- TLS auth reconcile tests --------------------------------------------

/// The first reconcile of a `KafkaUser` with TLS authentication
/// provisions the clients CA, which is the key Secret and the cert Secret,
/// the cert Secret of the user, and the ACLs for the `User:CN=<name>`
/// principal. It makes no SCRAM call.
// straight-line fixture; splitting hurts more than it helps
#[tokio::test]
async fn first_reconcile_tls_provisions_certs_and_acls() {
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        // Clients-CA key Secret doesn't exist yet.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        // Clients-CA cert Secret doesn't exist yet.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca-cert"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        // Per-user Secret doesn't exist yet.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        // Apply (create) the CA key Secret.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca-cert"),
            response: json_response(
                200,
                &clients_ca_cert_secret_body(
                    CLUSTER,
                    NS,
                    "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n",
                ),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca"),
            response: json_response(
                200,
                &clients_ca_key_secret_body(
                    CLUSTER,
                    NS,
                    "-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n",
                ),
            ),
        },
        // Apply (create) the per-user Secret.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(200, &tls_user_secret_body(USER, NS, "cert", "key", "ca")),
        },
        // Final status PATCH.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_tls(USER, vec![rule_topic("orders", &[AclOp::Read])]);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    // TLS path must NOT make SCRAM calls.
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::AlterUserScramCredentials { .. })),
        "TLS auth must skip SCRAM credentials: {calls:?}",
    );
    // ACL describe must use the CN= principal.
    assert!(
        calls.iter().any(|c| matches!(c,
            RecordedCall::DescribeAcls(f) if f.principal.as_deref() == Some("User:CN=alice"))),
        "expected DescribeAcls filtered by User:CN=alice, got {calls:?}",
    );
    // ACL create must use the CN= principal.
    let create = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::CreateAcls(v) => Some(v.clone()),
            _ => None,
        })
        .expect("CreateAcls must have been issued");
    assert!(
        create
            == vec![AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: "orders".into(),
                pattern_type: PatternType::Literal,
                principal: "User:CN=alice".into(),
                host: "*".into(),
                operation: AclOperation::Read,
                permission_type: PermissionType::Allow,
            }]
    );

    // Verify all four PATCH paths landed.
    let observed = state.take_observed();
    let patches: Vec<String> = observed
        .iter()
        .filter(|r| r.method() == Method::PATCH)
        .map(|r| r.uri().to_string())
        .collect();
    check!(
        patches
            .iter()
            .any(|u| u.contains(&format!("/secrets/{CLUSTER}-clients-ca-cert"))),
        "expected PATCH on clients-ca-cert Secret: {patches:?}",
    );
    check!(
        patches.iter().any(
            |u| u.contains(&format!("/secrets/{CLUSTER}-clients-ca")) && !u.contains("ca-cert")
        ),
        "expected PATCH on clients-ca key Secret: {patches:?}",
    );
    check!(
        patches
            .iter()
            .any(|u| u.contains(&format!("/secrets/{USER}"))),
        "expected PATCH on per-user Secret: {patches:?}",
    );
    check!(
        patches
            .iter()
            .any(|u| u.contains(&format!("/kafkausers/{USER}/status"))),
        "expected PATCH on KafkaUser status: {patches:?}",
    );
}

/// A TLS reconcile with an existing user Secret whose cert is far outside
/// the renewal window reuses that Secret. It issues no PATCH on the user
/// Secret.
#[tokio::test]
async fn tls_reconcile_reuses_existing_cert_when_not_near_expiry() {
    let ca = ca::generate_clients_ca("ca", 365).expect("ca");
    let user = ca::issue_user_cert(&ca.cert_pem, &ca.key_pem, USER, 365).expect("user cert");

    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca-cert"),
            response: json_response(200, &clients_ca_cert_secret_body(CLUSTER, NS, &ca.cert_pem)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca"),
            response: json_response(200, &clients_ca_key_secret_body(CLUSTER, NS, &ca.key_pem)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(
                200,
                &tls_user_secret_body(USER, NS, &user.cert_pem, &user.key_pem, &ca.cert_pem),
            ),
        },
        // Only the status PATCH is registered — no per-user Secret PATCH expected.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_tls(USER, vec![]);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let observed = state.take_observed();
    let secret_patches: Vec<String> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains(&format!("/secrets/{USER}"))
        })
        .map(|r| r.uri().to_string())
        .collect();
    assert!(
        secret_patches.is_empty(),
        "cert should be reused — no PATCH on per-user Secret expected, got: {secret_patches:?}",
    );
    // Status patch should still fire.
    assert!(
        observed.iter().any(|r| r.method() == Method::PATCH
            && r.uri()
                .to_string()
                .contains(&format!("/kafkausers/{USER}/status"))),
        "status PATCH must still fire",
    );
}

/// After clients-CA rotation prunes the old root, a still-valid leaf does not
/// need reissuing, but its Secret must stop advertising the pruned root.
#[tokio::test]
async fn tls_reconcile_after_ca_prune_synchronizes_only_active_root() {
    use base64::Engine as _;

    let old_ca = ca::generate_clients_ca("old-ca", 365).expect("old CA");
    let active_ca = ca::generate_clients_ca("active-ca", 365).expect("active CA");
    let user =
        ca::issue_user_cert(&active_ca.cert_pem, &active_ca.key_pem, USER, 365).expect("user cert");
    let stale_bundle = format!("{}{}", active_ca.cert_pem, old_ca.cert_pem);

    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca-cert"),
            response: json_response(
                200,
                &clients_ca_cert_secret_body(CLUSTER, NS, &active_ca.cert_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca"),
            response: json_response(
                200,
                &clients_ca_key_secret_body(CLUSTER, NS, &active_ca.key_pem),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(
                200,
                &tls_user_secret_body(USER, NS, &user.cert_pem, &user.key_pem, &stale_bundle),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(
                200,
                &tls_user_secret_body(USER, NS, &user.cert_pem, &user.key_pem, &active_ca.cert_pem),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let ctx = Arc::new(fixture_ctx(mock_client(&state, NS), NS));
    ctx.insert_admin_client_for_test(
        CLUSTER,
        Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new())),
    )
    .await;

    reconcile(Arc::new(ku_tls(USER, vec![])), ctx)
        .await
        .unwrap();

    let observed = state.take_observed();
    let user_patches: Vec<_> = observed
        .iter()
        .filter(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/secrets/{USER}"))
        })
        .collect();
    assert!(user_patches.len() == 1, "expected one CA-only patch");
    let body: serde_json::Value =
        serde_json::from_slice(user_patches[0].body()).expect("user Secret patch JSON");
    let data = body["data"].as_object().expect("Secret data patch");
    assert!(data.len() == 1 && data.contains_key("ca.crt"));
    let patched_ca = base64::engine::general_purpose::STANDARD
        .decode(data["ca.crt"].as_str().expect("base64 ca.crt"))
        .expect("ca.crt decodes");
    assert!(patched_ca == active_ca.cert_pem.as_bytes());
    assert!(state.remaining_rules() == 0);
}

/// A TLS reconcile with an existing cert inside the renewal window issues
/// a new cert. It makes exactly one PATCH on the Secret of the user.
#[tokio::test]
async fn tls_reconcile_reissues_cert_near_expiry() {
    let ca = ca::generate_clients_ca("ca", 365).expect("ca");
    // 1-day validity: well inside the default 30-day renewal window.
    let user = ca::issue_user_cert(&ca.cert_pem, &ca.key_pem, USER, 1).expect("user cert");

    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca-cert"),
            response: json_response(200, &clients_ca_cert_secret_body(CLUSTER, NS, &ca.cert_pem)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca"),
            response: json_response(200, &clients_ca_key_secret_body(CLUSTER, NS, &ca.key_pem)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(
                200,
                &tls_user_secret_body(USER, NS, &user.cert_pem, &user.key_pem, &ca.cert_pem),
            ),
        },
        // Reissue PATCH.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(200, &tls_user_secret_body(USER, NS, "cert", "key", "ca")),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_tls(USER, vec![]);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let observed = state.take_observed();
    let secret_patches: Vec<String> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains(&format!("/secrets/{USER}"))
        })
        .map(|r| r.uri().to_string())
        .collect();
    assert!(
        secret_patches.len() == 1,
        "near-expiry cert must be reissued exactly once: {secret_patches:?}"
    );
}

/// The finalizer cleanup of a TLS user filters the ACL deletes by the
/// `User:CN=<name>` principal, and not by the plain `User:<name>` SCRAM
/// form.
#[tokio::test]
async fn tls_finalizer_filters_acls_by_dn() {
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        // Finalizer-removal PATCH on the KafkaUser itself.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let mut ku = ku_tls(USER, vec![]);
    ku.metadata.deletion_timestamp = Some(Time("2026-05-18T00:00:00Z".parse().unwrap()));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    // TLS finalizer must NOT issue a SCRAM delete (gated on auth type).
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::AlterUserScramCredentials { .. })),
        "TLS finalizer must not call SCRAM delete: {calls:?}",
    );
    // ACL delete filter must use `User:CN=alice`.
    let filters = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::DeleteAcls(f) => Some(f.clone()),
            _ => None,
        })
        .expect("DeleteAcls must have been issued");
    assert!(!filters.is_empty(), "at least one filter expected");
    assert!(
        filters[0].principal.as_deref() == Some("User:CN=alice"),
        "TLS finalizer must filter by CN= principal: {filters:?}"
    );
}

/// A TLS user with quotas keys the broker quota calls by the DN,
/// `CN=alice`, and not by the plain name.
#[tokio::test]
async fn tls_user_with_quotas_alters_quotas_by_dn() {
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca-cert"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{USER}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca-cert"),
            response: json_response(200, &clients_ca_cert_secret_body(CLUSTER, NS, "cert")),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{CLUSTER}-clients-ca"),
            response: json_response(200, &clients_ca_key_secret_body(CLUSTER, NS, "key")),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(200, &tls_user_secret_body(USER, NS, "cert", "key", "ca")),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let quotas = KafkaUserQuotas {
        producer_byte_rate: Some(1_048_576),
        ..Default::default()
    };
    let ku = ku_tls_full(USER, vec![], Some(quotas), TlsAuth::default());
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    // DescribeUserQuotas must be keyed by `CN=alice`, not `alice`.
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, RecordedCall::DescribeUserQuotas(u) if u == "CN=alice")),
        "expected DescribeUserQuotas keyed by CN=alice, got {calls:?}",
    );
    // AlterUserQuotas must be keyed by `CN=alice` and carry the Set op.
    let (username, ops) = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::AlterUserQuotas { username, ops, .. } => {
                Some((username.clone(), ops.clone()))
            }
            _ => None,
        })
        .expect("AlterUserQuotas must have been issued");
    assert!(
        username == "CN=alice",
        "must be keyed by DN, got {username}"
    );
    assert!(
        ops.iter().any(|op| matches!(
            op,
            QuotaOp::Set { key, value }
                if key == "producer_byte_rate" && (*value - 1_048_576.0).abs() < f64::EPSILON
        )),
        "expected Set producer_byte_rate=1048576, got {ops:?}",
    );
}
