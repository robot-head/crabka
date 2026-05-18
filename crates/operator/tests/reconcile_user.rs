//! Slice 36: reconcile-level tests for the `KafkaUser` controller.

use std::collections::BTreeMap;
use std::sync::Arc;

use crabka_client_admin::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use crabka_operator::controller::user::reconcile;
use crabka_operator::crd::{
    AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule, Authentication,
    Authorization, KafkaUser, KafkaUserSpec, ScramSha512Auth, SimpleAuthorization,
};
use http::{Method, Response};
use serde_json::json;

#[path = "shared/mod.rs"]
mod shared;

use shared::fake_admin::{FakeAdminClient, RecordedCall};
use shared::{MockRule, MockState, fixture_ctx, json_response, mock_client, not_found_body};

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
    let mut ku = KafkaUser::new(
        name,
        KafkaUserSpec {
            authentication: Authentication::ScramSha512(ScramSha512Auth::default()),
            authorization: if acls.is_empty() {
                None
            } else {
                Some(Authorization::Simple(SimpleAuthorization { acls }))
            },
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

/// Slice 36: `KafkaUser` with cluster label, no Secret yet → reconcile
/// creates the Secret, upserts SCRAM, applies ACLs, sets Ready=True.
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
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_with_finalizer(
        USER,
        vec![rule_topic("orders", &[AclOp::Read, AclOp::Describe])],
    );
    reconcile(Arc::new(ku), ctx).await.unwrap();

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
    assert_eq!(create.len(), 2, "two ops fan out into two ACL entries");
    let ops: Vec<AclOperation> = create.iter().map(|e| e.operation).collect();
    assert!(ops.contains(&AclOperation::Read));
    assert!(ops.contains(&AclOperation::Describe));
    for e in &create {
        assert_eq!(e.resource_type, ResourceType::Topic);
        assert_eq!(e.resource_name, "orders");
        assert_eq!(e.pattern_type, PatternType::Literal);
        assert_eq!(e.principal, "User:alice");
        assert_eq!(e.host, "*");
        assert_eq!(e.permission_type, PermissionType::Allow);
    }

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
    assert_eq!(body["status"]["conditions"][0]["status"], "True");
    assert_eq!(body["status"]["conditions"][0]["reason"], "Ready");
    assert_eq!(body["status"]["username"], USER);
    assert_eq!(body["status"]["secret"], USER);
    assert_eq!(body["status"]["scramSha512"], true);
}

/// Slice 36: reconcile with existing matching ACLs → no `CreateAcls` /
/// `DeleteAcls` calls.
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

/// Slice 36: reconcile drops out-of-band ACLs not in spec (CRD wins).
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

    // Spec declares zero ACLs.
    let ku = ku_with_finalizer(USER, vec![]);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let delete = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::DeleteAcls(filters) => Some(filters.clone()),
            _ => None,
        })
        .expect("DeleteAcls must have been issued");
    assert_eq!(delete.len(), 1);
    assert_eq!(delete[0].resource_name.as_deref(), Some("secrets"));

    // The reconciler scoped the delete by every axis (no broad filter
    // collapsing into "delete everything for this principal").
    assert!(delete[0].operation.is_some());
    assert!(delete[0].permission_type.is_some());

    // Verify the store is empty after the reconcile completed.
    let store = fake_for_assert.lock().await.acls.lock().unwrap().clone();
    assert!(
        store.is_empty(),
        "delete should have removed every ACL, got: {store:?}",
    );
}
