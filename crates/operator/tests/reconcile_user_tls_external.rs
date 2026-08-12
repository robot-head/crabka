//! Reconcile-level tests for the `tls-external` `KafkaUser` arm.
//!
//! These users have no credentials. The operator reconciles their ACLs and
//! quotas under the bare-name principal `User:<metadata.name>`. The operator
//! creates no Secret for them and issues no cert.

use std::{collections::BTreeMap, sync::Arc};

use assert2::{assert, check};
use crabka_client_admin::{
    AclEntry, AclOperation, PatternType, PermissionType, QuotaOp, ResourceType,
};
use crabka_operator::{
    controller::user::reconcile,
    crd::{
        AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule,
        Authentication, KafkaUser, KafkaUserAuthorization as Authorization, KafkaUserQuotas,
        KafkaUserSimpleAuthorization as SimpleAuthorization, KafkaUserSpec,
    },
};
use http::Method;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use serde_json::json;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState,
    fake_admin::{FakeAdminClient, RecordedCall},
    fixture_ctx, json_response, mock_client,
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
            "authentication": {"type": "tls-external"},
        },
        "status": {"conditions": []}
    })
}

/// Build a `KafkaUser` with `authentication.type: tls-external`, the cluster
/// label, the user-finalizer pre-installed, and the supplied ACL rules and
/// quota block.
fn ku_external_full(name: &str, acls: Vec<AclRule>, quotas: Option<KafkaUserQuotas>) -> KafkaUser {
    let mut ku = KafkaUser::new(
        name,
        KafkaUserSpec {
            authentication: Authentication::TlsExternal,
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

fn ku_external(name: &str, acls: Vec<AclRule>) -> KafkaUser {
    ku_external_full(name, acls, None)
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

/// Minimum kube-mock rule set for a non-finalizer `tls-external` reconcile:
/// GET the Kafka and PATCH the status. The operator must not touch any Secret.
/// The FIFO mock falls through to 404 on any other path, and that 404 becomes
/// a reconcile error.
fn external_rules() -> Vec<MockRule> {
    vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ]
}

/// 1. `tls-external` user reconcile must not PATCH or POST any
///    Secret. The kube observation log is the source of truth.
#[tokio::test]
async fn tls_external_user_creates_no_secret() {
    let state = MockState::new(external_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_external(USER, vec![rule_topic("orders", &[AclOp::Read])]);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let observed = state.take_observed();
    let secret_touches: Vec<String> = observed
        .iter()
        .filter(|r| {
            matches!(r.method(), &Method::PATCH | &Method::POST)
                && r.uri().to_string().contains("/secrets/")
        })
        .map(|r| r.uri().to_string())
        .collect();
    assert!(
        secret_touches.is_empty(),
        "tls-external must not PATCH/POST any Secret, got: {secret_touches:?}",
    );

    // The FIFO admin mock must NOT have seen any SCRAM call.
    let calls = fake_for_assert.lock().await.calls();
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::AlterUserScramCredentials { .. })),
        "tls-external must not call AlterUserScramCredentials: {calls:?}",
    );
    // The only admin calls expected are ACL describe + (when the spec
    // declares ACLs) create.
    for call in &calls {
        assert!(
            matches!(
                call,
                RecordedCall::DescribeAcls(_)
                    | RecordedCall::CreateAcls(_)
                    | RecordedCall::DeleteAcls(_)
                    | RecordedCall::DescribeUserQuotas(_)
                    | RecordedCall::AlterUserQuotas { .. }
            ),
            "unexpected admin call for tls-external: {call:?}",
        );
    }
}

/// 2. `CreateAcls` for a `tls-external` user must use the bare-name
///    principal `User:<metadata.name>`. This is the same shape as SCRAM,
///    *not* the `User:CN=<name>` TLS shape.
#[tokio::test]
async fn tls_external_user_reconciles_acls_under_bare_name_principal() {
    let state = MockState::new(external_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_external(
        USER,
        vec![rule_topic("orders", &[AclOp::Read, AclOp::Describe])],
    );
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    // DescribeAcls must filter by the bare-name principal.
    assert!(
        calls.iter().any(|c| matches!(
            c,
            RecordedCall::DescribeAcls(f) if f.principal.as_deref() == Some("User:alice")
        )),
        "expected DescribeAcls filtered by User:alice, got {calls:?}",
    );
    // CreateAcls must use the bare-name principal on every entry.
    let create = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::CreateAcls(v) => Some(v.clone()),
            _ => None,
        })
        .expect("CreateAcls must have been issued");
    // Read+Describe fan out into two entries (the BTreeSet diff hands
    // them over in Ord order: Read before Describe), each under the
    // bare-name principal `User:alice`.
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
        "tls-external must fan Read+Describe out under the bare-name principal",
    );
}

/// 3. `AlterClientQuotas` for a `tls-external` user keys by the bare
///    `metadata.name`. The broker stores quotas under the username without
///    the `User:` prefix. For TLS users this is `CN=<name>`. For SCRAM and
///    `tls-external` users it is `<name>`.
#[tokio::test]
async fn tls_external_user_reconciles_quotas_under_bare_name_principal() {
    let state = MockState::new(external_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let quotas = KafkaUserQuotas {
        producer_byte_rate: Some(1_048_576),
        ..Default::default()
    };
    let ku = ku_external_full(USER, vec![], Some(quotas));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    // DescribeUserQuotas must be keyed by `alice`, not `User:alice` or `CN=alice`.
    assert!(
        calls
            .iter()
            .any(|c| matches!(c, RecordedCall::DescribeUserQuotas(u) if u == USER)),
        "expected DescribeUserQuotas keyed by `{USER}`, got {calls:?}",
    );
    // AlterUserQuotas must be keyed by `alice` and carry the Set op.
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
        username == USER,
        "tls-external quotas must be keyed by bare name, got `{username}`"
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

/// 4. Status after reconcile must report `external=true`,
///    `tlsPrincipal="User:<name>"`, `secret=null` for no Secret,
///    `scramSha512=false`, and `tls=false`.
#[tokio::test]
async fn tls_external_user_status_reports_external_true_and_tls_principal_and_no_secret() {
    let state = MockState::new(external_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_external(USER, vec![rule_topic("orders", &[AclOp::Read])]);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let observed = state.take_observed();
    let status = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    let s = &body["status"];
    check!(s["conditions"][0]["status"] == "True");
    check!(s["conditions"][0]["reason"] == "Ready");
    check!(s["external"] == true, "external must be true: {body}");
    check!(
        s["tlsPrincipal"] == "User:alice",
        "tlsPrincipal must pin the bare-name principal: {body}"
    );
    check!(s["secret"] == serde_json::Value::Null, "no Secret: {body}");
    check!(s["scramSha512"] == false, "no SCRAM: {body}");
    check!(s["tls"] == false, "no operator-issued TLS cert: {body}");
}

/// 5. A minimal `tls-external` user with no authorization and no quotas
///    still reaches `Ready=True` without touching broker ACLs or quotas.
#[tokio::test]
async fn tls_external_user_with_no_authorization_and_no_quotas_still_reaches_ready_true() {
    let state = MockState::new(external_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    let existing_acl = AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "operator-unmanaged".into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    };
    fake.acls.lock().unwrap().insert(existing_acl.clone());
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_external(USER, vec![]);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    check!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::DescribeAcls(_))),
        "authorization=None must skip ACL reconciliation: {calls:?}",
    );
    check!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::CreateAcls(_))),
        "no CreateAcls expected when spec declares no ACLs: {calls:?}",
    );
    check!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::DescribeUserQuotas(_))),
        "no DescribeUserQuotas expected when spec.quotas is None: {calls:?}",
    );
    check!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::AlterUserQuotas { .. })),
        "no AlterUserQuotas expected when spec.quotas is None: {calls:?}",
    );
    let retained = fake_for_assert.lock().await.acls.lock().unwrap().clone();
    check!(
        retained.contains(&existing_acl),
        "authorization=None must retain pre-existing broker ACLs: {retained:?}",
    );

    // Final status PATCH must land Ready=True.
    let observed = state.take_observed();
    let status = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    check!(body["status"]["conditions"][0]["status"] == "True");
    check!(body["status"]["conditions"][0]["reason"] == "Ready");
    check!(body["status"]["external"] == true);
}

/// 6. Finalizer cleanup for a `tls-external` user must not call
///    `AlterUserScramCredentials`, because no SCRAM credential exists. It
///    can issue `DeleteAcls` and quota cleanup. Both are best-effort.
#[tokio::test]
async fn tls_external_user_finalizer_does_not_call_alter_user_scram_credentials() {
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        // Finalizer-removal PATCH on the `KafkaUser` itself.
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

    let mut ku = ku_external(USER, vec![]);
    ku.metadata.deletion_timestamp = Some(Time("2026-05-18T00:00:00Z".parse().unwrap()));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::AlterUserScramCredentials { .. })),
        "tls-external finalizer must not call AlterUserScramCredentials: {calls:?}",
    );

    // ACL delete filter must scope by the bare-name principal.
    let filters = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::DeleteAcls(f) => Some(f.clone()),
            _ => None,
        })
        .expect("DeleteAcls must have been issued during finalizer");
    assert!(!filters.is_empty(), "at least one filter expected");
    assert!(
        filters[0].principal.as_deref() == Some("User:alice"),
        "tls-external finalizer must filter ACLs by `User:<name>`: {filters:?}"
    );

    // The kube observation log must show the finalizer-removal PATCH on
    // the user object (and nothing pointing at a Secret).
    let observed = state.take_observed();
    let secret_touches: Vec<String> = observed
        .iter()
        .filter(|r| r.uri().to_string().contains("/secrets/"))
        .map(|r| format!("{} {}", r.method(), r.uri()))
        .collect();
    assert!(
        secret_touches.is_empty(),
        "tls-external finalizer must not touch any Secret, got: {secret_touches:?}",
    );
}
