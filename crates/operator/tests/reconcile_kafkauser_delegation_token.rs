//! Reconcile-level integration tests for the delegation-token `KafkaUser` arm.
//!
//! These tests exercise the production dispatch path in
//! `controller::user::reconcile`. The harness is the same as
//! `reconcile_user.rs`: a mock kube transport and an injected
//! `FakeAdminClient`.
//!
//! The delegation-token RPCs of the fake are in-memory KIP-48. `Create` mints
//! a fresh token with `expiry_ts = now + 7d` and `max_ts = now + 30d`. `Renew`
//! advances `expiry_ts` to `min(now + 7d, max_ts)`. `Expire` tombstones the
//! matching entry. The fake records each call, so the tests can assert on the
//! exact RPC sequence that the dispatch emits.
//!
//! These tests use a mock and not a real broker because the operator's
//! per-resource reconcile path is unit-isolated from broker I/O. The
//! `DelegationTokenAdmin` trait gives a substitution seam. The `crabka_broker`
//! integration tests cover the broker-side act-as wire path.

use std::{collections::BTreeMap, sync::Arc};

use assert2::{assert, check};
use crabka_client_admin::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use crabka_operator::{
    controller::user::reconcile,
    crd::{
        AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule,
        Authentication, DelegationTokenAuth, KafkaUser, KafkaUserAuthorization as Authorization,
        KafkaUserQuotas, KafkaUserSimpleAuthorization as SimpleAuthorization, KafkaUserSpec,
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

/// Echo body for the user-Secret PATCH. The operator examines it only to
/// confirm that the apply succeeded. The test asserts on the request body.
fn secret_body(name: &str, namespace: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "secret-uid" },
        "type": "Opaque",
        "data": {}
    })
}

/// Echo body for the `KafkaUser` status PATCH. kube-rs requires that the
/// response deserializes back into a `KafkaUser`, so this body is a minimal
/// valid shape.
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
            "authentication": {"type": "delegation-token"},
        },
        "status": {"conditions": []}
    })
}

/// Build a delegation-token `KafkaUser` with the finalizer already set.
fn ku_delegation_token(name: &str, auth: DelegationTokenAuth) -> KafkaUser {
    let mut ku = KafkaUser::new(
        name,
        KafkaUserSpec {
            authentication: Authentication::DelegationToken(auth),
            authorization: None,
            quotas: None,
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

/// FIFO mock rules for one happy-path reconcile of a delegation-token user:
/// Kafka GET, pending status, Secret PATCH, token identity status, then final
/// access status.
fn happy_path_rules() -> Vec<MockRule> {
    vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{USER}"),
            response: json_response(200, &secret_body(USER, NS)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{USER}/status"),
            response: json_response(200, &user_body(USER, NS)),
        },
    ]
}

// ─── Test 1: first reconcile mints token + writes Secret + status ───────

/// Spec §3.2: a fresh delegation-token user with no existing token gives
/// `Describe → pending → Create → Secret apply → status patch`. The Secret carries the
/// four KIP-48 keys. The status carries the token id and expiry, plus the
/// `Ready=True` and `TokenIssued=True` conditions.
#[tokio::test]
async fn delegation_token_user_reconcile_creates_secret_and_status() {
    let state = MockState::new(happy_path_rules());
    let client = mock_client(&state, NS);
    let mut ctx = fixture_ctx(client, NS);
    let config = Arc::get_mut(&mut ctx.config).expect("fixture owns operator config");
    config.delegation_token_min_requeue = crabka_units::days(1);
    config.delegation_token_max_requeue = crabka_units::days(1);
    config.controller_drift_requeue = crabka_units::millis(1_234);
    let ctx = Arc::new(ctx);

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_delegation_token(
        USER,
        DelegationTokenAuth {
            renewers: vec!["User:bob".into()],
            max_lifetime: None,
            renew_before_expiry: None,
        },
    );
    let action = reconcile(Arc::new(ku), ctx).await.unwrap();
    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(1_234)),
        "access drift polling must bound a distant token-renewal deadline",
    );

    // ── Admin-call shape ────────────────────────────────────────────
    let calls = fake_for_assert.lock().await.calls();
    // Describe (empty) → Create.
    assert!(
        calls.iter().any(|c| matches!(
            c,
            RecordedCall::DescribeDelegationTokensOwnedBy { owner_principal }
                if owner_principal == "User:alice"
        )),
        "expected DescribeDelegationTokensOwnedBy for User:alice, got: {calls:?}",
    );
    let create_call = calls.iter().find_map(|c| match c {
        RecordedCall::CreateDelegationToken {
            owner_principal_name,
            renewers,
            max_lifetime,
        } => Some((
            owner_principal_name.clone(),
            renewers.clone(),
            *max_lifetime,
        )),
        _ => None,
    });
    // Owner principal name carries no `User:` prefix; unset
    // spec.max_lifetime → `None` (broker default).
    assert!(
        create_call == Some(("alice".to_string(), vec!["User:bob".to_string()], None)),
        "CreateDelegationToken must have been issued with these exact args",
    );

    // ── Secret PATCH body: four KIP-48 keys ────────────────────────
    let observed = state.take_observed();
    let secret_patch = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains(&format!("/secrets/{USER}"))
        })
        .expect("Secret PATCH must have been observed");
    let body: serde_json::Value = serde_json::from_slice(secret_patch.body()).unwrap();
    let data = body["data"].as_object().expect("data object");
    for key in ["token-id", "hmac", "password", "sasl.jaas.config"] {
        assert!(
            data.contains_key(key),
            "Secret.data missing {key}: {data:?}",
        );
    }

    // ── Status PATCHes: pending, token identity, access-complete Ready
    let status_patches: Vec<_> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
        .collect();
    assert!(status_patches.len() == 3);
    let pending_body: serde_json::Value = serde_json::from_slice(status_patches[0].body()).unwrap();
    assert!(
        pending_body["status"]["conditions"]
            .as_array()
            .expect("pending conditions")
            .iter()
            .any(|condition| condition["type"] == "Ready"
                && condition["status"] == "False"
                && condition["reason"] == "TokenPending")
    );
    let body: serde_json::Value = serde_json::from_slice(status_patches[1].body()).unwrap();
    let status = &body["status"];
    assert!(
        status["delegationTokenId"].is_string(),
        "delegationTokenId missing: {status}",
    );
    let expiry = status["delegationTokenExpiryTimestampMs"]
        .as_i64()
        .expect("delegationTokenExpiryTimestampMs is i64");
    assert!(
        expiry > 0,
        "delegationTokenExpiryTimestampMs must be positive, got {expiry}",
    );
    let max_ts = status["delegationTokenMaxTimestampMs"]
        .as_i64()
        .expect("delegationTokenMaxTimestampMs is i64");
    assert!(max_ts >= expiry, "max_ts ({max_ts}) >= expiry ({expiry})");

    assert!(
        status["conditions"]
            .as_array()
            .expect("identity pending conditions")
            .iter()
            .any(|condition| condition["type"] == "Ready"
                && condition["status"] == "False"
                && condition["reason"] == "TokenPending")
    );
    let final_body: serde_json::Value = serde_json::from_slice(status_patches[2].body()).unwrap();
    let final_conditions = final_body["status"]["conditions"]
        .as_array()
        .expect("final conditions array");
    assert!(
        final_conditions
            .iter()
            .any(|c| c["type"] == "Ready" && c["status"] == "True" && c["reason"] == "TokenReady"),
        "Ready must publish only after access sync: {final_conditions:?}",
    );
    assert!(
        final_conditions.iter().any(|c| c["type"] == "TokenIssued"
            && c["status"] == "True"
            && c["reason"] == "Issued")
    );
    assert!(final_body["status"]["observedGeneration"] == 1);
}

#[tokio::test]
async fn delegation_token_user_reconciles_acls_without_replacing_token_status_or_action() {
    let state = MockState::new(happy_path_rules());
    let client = mock_client(&state, NS);
    let mut ctx = fixture_ctx(client, NS);
    let config = Arc::get_mut(&mut ctx.config).expect("fixture owns operator config");
    config.delegation_token_min_requeue = crabka_units::millis(2_345);
    config.delegation_token_max_requeue = crabka_units::millis(2_345);
    let ctx = Arc::new(ctx);

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let mut ku = ku_delegation_token(USER, DelegationTokenAuth::default());
    ku.spec.authorization = Some(Authorization::Simple(SimpleAuthorization {
        acls: vec![AclRule {
            resource: AclResource {
                kind: AclResourceKind::Topic,
                name: "delegation-token-test".into(),
                pattern_type: AclPatternType::Literal,
            },
            operations: vec![AclOp::Describe, AclOp::Write],
            host: "*".into(),
            permission: AclPermission::Allow,
        }],
    }));
    ku.spec.quotas = Some(KafkaUserQuotas {
        producer_byte_rate: Some(1_024),
        ..Default::default()
    });

    let action = reconcile(Arc::new(ku), ctx).await.unwrap();

    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(2_345)),
        "ACL reconciliation must preserve the token's expiry-driven action",
    );
    let calls = fake_for_assert.lock().await.calls();
    assert!(
        calls.iter().any(|call| matches!(
            call,
            RecordedCall::DescribeAcls(filter)
                if filter.principal.as_deref() == Some("User:alice")
        )),
        "expected DescribeAcls for User:alice, got {calls:?}",
    );
    let created = calls
        .iter()
        .find_map(|call| match call {
            RecordedCall::CreateAcls(entries) => Some(entries.clone()),
            _ => None,
        })
        .expect("CreateAcls must have been issued");
    assert!(
        created
            == vec![
                AclEntry {
                    resource_type: ResourceType::Topic,
                    resource_name: "delegation-token-test".into(),
                    pattern_type: PatternType::Literal,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: AclOperation::Write,
                    permission_type: PermissionType::Allow,
                },
                AclEntry {
                    resource_type: ResourceType::Topic,
                    resource_name: "delegation-token-test".into(),
                    pattern_type: PatternType::Literal,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: AclOperation::Describe,
                    permission_type: PermissionType::Allow,
                },
            ],
        "the requested topic ACLs must be created for the token owner",
    );

    let observed = state.take_observed();
    let status_patches: Vec<_> = observed
        .iter()
        .filter(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
        .collect();
    assert!(
        status_patches.len() == 3,
        "expected pending, identity, and final access status patches"
    );
    let pending_body: serde_json::Value = serde_json::from_slice(status_patches[0].body()).unwrap();
    let conditions = pending_body["status"]["conditions"]
        .as_array()
        .expect("conditions array");
    assert!(conditions[0]["status"] == "False");
    assert!(conditions[0]["reason"] == "TokenPending");
    let token_body: serde_json::Value = serde_json::from_slice(status_patches[1].body()).unwrap();
    assert!(
        token_body["status"]["conditions"]
            .as_array()
            .expect("identity pending conditions")
            .iter()
            .any(|condition| condition["type"] == "Ready" && condition["status"] == "False")
    );
    let access_body: serde_json::Value = serde_json::from_slice(status_patches[2].body()).unwrap();
    assert!(access_body["status"]["observedGeneration"] == 1);
    assert!(access_body["status"]["quotasInSync"] == true);
    let final_conditions = access_body["status"]["conditions"]
        .as_array()
        .expect("final conditions array");
    assert!(final_conditions[0]["status"] == "True");
    assert!(final_conditions[0]["reason"] == "TokenReady");
    assert!(
        final_conditions
            .iter()
            .any(|condition| condition["type"] == "TokenIssued"),
        "final access status must retain token-specific conditions",
    );
}

#[tokio::test]
async fn delegation_token_user_acl_failure_never_publishes_ready_or_observed_generation() {
    let state = MockState::new(happy_path_rules());
    let client = mock_client(&state, NS);
    let mut ctx = fixture_ctx(client, NS);
    let config = Arc::get_mut(&mut ctx.config).expect("fixture owns operator config");
    config.delegation_token_min_requeue = crabka_units::millis(2_000);
    config.delegation_token_max_requeue = crabka_units::millis(2_000);
    config.controller_error_requeue = crabka_units::millis(15_000);
    let ctx = Arc::new(ctx);

    let fake_admin = FakeAdminClient::new();
    fake_admin.inject_create_acls_broker_error(
        29,
        "TOPIC_AUTHORIZATION_FAILED",
        Some("injected ACL failure".into()),
    );
    let fake = Arc::new(tokio::sync::Mutex::new(fake_admin));
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let mut ku = ku_delegation_token(USER, DelegationTokenAuth::default());
    ku.spec.authorization = Some(Authorization::Simple(SimpleAuthorization {
        acls: vec![AclRule {
            resource: AclResource {
                kind: AclResourceKind::Topic,
                name: "delegation-token-test".into(),
                pattern_type: AclPatternType::Literal,
            },
            operations: vec![AclOp::Write],
            host: "*".into(),
            permission: AclPermission::Allow,
        }],
    }));

    let action = reconcile(Arc::new(ku), ctx).await.unwrap();
    assert!(
        action == kube::runtime::controller::Action::requeue(std::time::Duration::from_secs(2)),
        "access failure must not defer an earlier token deadline",
    );

    let observed = state.take_observed();
    let status_patches: Vec<serde_json::Value> = observed
        .iter()
        .filter(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
        .map(|request| serde_json::from_slice(request.body()).unwrap())
        .collect();
    assert!(status_patches.len() == 3);
    assert!(
        [&status_patches[0], &status_patches[1], &status_patches[2]]
            .iter()
            .all(|body| {
                body["status"]["conditions"]
                    .as_array()
                    .expect("conditions array")
                    .iter()
                    .all(|condition| condition["type"] != "Ready" || condition["status"] == "False")
            })
    );

    let failure_status = status_patches[2]["status"]
        .as_object()
        .expect("failure status object");
    assert!(
        failure_status.len() == 1 && failure_status.contains_key("conditions"),
        "failure patch must retain token identity by changing conditions only: {failure_status:?}",
    );
    let conditions = failure_status["conditions"]
        .as_array()
        .expect("failure conditions array");
    assert!(
        conditions
            .iter()
            .any(|condition| condition["type"] == "Ready"
                && condition["status"] == "False"
                && condition["reason"] == "BrokerError")
    );
    assert!(
        conditions
            .iter()
            .any(|condition| condition["type"] == "TokenIssued" && condition["status"] == "True")
    );
    assert!(
        conditions
            .iter()
            .any(|condition| condition["type"] == "TokenExpiring"),
        "access failure must retain token-specific conditions",
    );
    assert!(failure_status.get("observedGeneration").is_none());
}

// ─── Test 2: renewal fires when remaining lifetime ≤ threshold ──────────

/// Spec §3.2: with `renew_before_expiry_ms = 7d` and a default 7d token
/// lifetime, `expiry - now <= 7d` is always true, so every reconcile runs the
/// Renew path. The test reconciles twice. It asserts that the second pass
/// calls Renew and not Create, and that the post-renew expiry has advanced or
/// held at `max_timestamp_ms` at the ceiling.
#[tokio::test]
async fn delegation_token_user_reconcile_renews_when_within_threshold() {
    let mut rules = happy_path_rules();
    // Second reconcile re-uses the same FIFO sequence.
    rules.extend(happy_path_rules());
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    // 7d renew_before with 7d default lifetime → renewal always fires.
    let auth = DelegationTokenAuth {
        renewers: vec![],
        max_lifetime: None,
        renew_before_expiry: Some(crabka_units::days(7)),
    };

    // ── Pass 1: Create + status patch ──────────────────────────────
    let ku = ku_delegation_token(USER, auth.clone());
    reconcile(Arc::new(ku), ctx.clone()).await.unwrap();

    let observed_first = state.take_observed();
    let status_first = observed_first
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
        .nth(1)
        .expect("status PATCH from pass 1");
    let body: serde_json::Value = serde_json::from_slice(status_first.body()).unwrap();
    let first_expiry = body["status"]["delegationTokenExpiryTimestampMs"]
        .as_i64()
        .expect("first expiry");
    let first_max = body["status"]["delegationTokenMaxTimestampMs"]
        .as_i64()
        .expect("first max");

    // Sleep long enough that `now` advances past 1ms on the wall clock.
    // 20ms gives us comfortable headroom against scheduler jitter.
    // real-time wait (not a progress poll): the renewed token's expiry is derived
    // from wall-clock now(), which must actually advance between passes;
    // yield_now() cannot move the clock forward.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // ── Pass 2: Describe finds existing → Renew ────────────────────
    let ku = ku_delegation_token(USER, auth);
    reconcile(Arc::new(ku), ctx).await.unwrap();

    let observed_second = state.take_observed();
    let status_second = observed_second
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
        .nth(1)
        .expect("status PATCH from pass 2");
    let body: serde_json::Value = serde_json::from_slice(status_second.body()).unwrap();
    let second_expiry = body["status"]["delegationTokenExpiryTimestampMs"]
        .as_i64()
        .expect("second expiry");

    // ── Admin-call shape: Describe→Create then Describe→Renew ──────
    let calls = fake_for_assert.lock().await.calls();
    let renews = calls
        .iter()
        .filter(|c| matches!(c, RecordedCall::RenewDelegationToken { .. }))
        .count();
    assert!(
        renews == 1,
        "expected exactly one Renew call, got: {calls:?}"
    );
    let creates = calls
        .iter()
        .filter(|c| matches!(c, RecordedCall::CreateDelegationToken { .. }))
        .count();
    check!(
        creates == 1,
        "expected exactly one Create call, got: {calls:?}"
    );

    // ── Expiry monotonically advances (or holds at `max_timestamp_ms`).
    check!(
        second_expiry >= first_expiry,
        "renew must not move expiry backwards: first={first_expiry}, second={second_expiry}",
    );
    check!(
        second_expiry <= first_max,
        "renew must not exceed max_timestamp_ms: second={second_expiry}, max={first_max}",
    );
}

// ─── Test 3: deletion expires the token and removes the Secret ──────────

/// Spec §3.2: a delete of the `KafkaUser`, with `deletion_timestamp` set,
/// triggers the finalizer. The finalizer calls `expire_owned_tokens`. That
/// function Describe-lists the tokens owned by `User:<name>` and Expires each
/// one. The finalizer then removes itself. The owner-references on the Secret
/// then cascade the Secret delete. The test verifies that the fake's token
/// store is empty and that the finalizer-removal PATCH landed.
#[tokio::test]
async fn delegation_token_user_deletion_expires_token_and_removes_secret() {
    // Pass 1: provision the token via the happy-path reconcile.
    let mut rules = happy_path_rules();
    // Pass 2 (deletion): GET Kafka, then PATCH on the KafkaUser to
    // remove the finalizer. (The finalizer arm never touches the user's
    // Secret directly — owner-references cascade that delete.)
    rules.push(MockRule {
        method: Method::GET,
        path_substr: format!("/kafkas/{CLUSTER}"),
        response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkausers/{USER}"),
        response: json_response(200, &user_body(USER, NS)),
    });
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    // ── Pass 1: provision ─────────────────────────────────────────
    let ku = ku_delegation_token(USER, DelegationTokenAuth::default());
    reconcile(Arc::new(ku), ctx.clone()).await.unwrap();

    // Sanity: token exists in the fake.
    {
        let store = fake_for_assert.lock().await;
        let tokens = store.delegation_tokens.lock().unwrap();
        assert!(tokens.len() == 1, "pass 1 must mint exactly one token");
    }

    // ── Pass 2: delete ────────────────────────────────────────────
    let mut ku = ku_delegation_token(USER, DelegationTokenAuth::default());
    ku.metadata.deletion_timestamp = Some(Time("2026-05-25T00:00:00Z".parse().unwrap()));
    reconcile(Arc::new(ku), ctx).await.unwrap();

    // ── Admin-call shape: Describe + Expire ───────────────────────
    let calls = fake_for_assert.lock().await.calls();
    // Two Describes total: pass-1 Describe (empty) before Create, and
    // pass-2 Describe inside `expire_owned_tokens` before Expire.
    let describes = calls
        .iter()
        .filter(|c| {
            matches!(
                c,
                RecordedCall::DescribeDelegationTokensOwnedBy { owner_principal }
                    if owner_principal == "User:alice"
            )
        })
        .count();
    assert!(
        describes == 2,
        "expected 2 Describes (provision + finalizer), got: {calls:?}"
    );
    let expires = calls
        .iter()
        .filter(|c| matches!(c, RecordedCall::ExpireDelegationToken { .. }))
        .count();
    assert!(
        expires == 1,
        "expected exactly one Expire from the finalizer, got: {calls:?}"
    );

    // ── Token store is now empty (Expire dropped the entry). ──────
    let store = fake_for_assert.lock().await;
    let owned = store
        .delegation_tokens
        .lock()
        .unwrap()
        .iter()
        .filter(|t| t.owner.principal_type == "User" && t.owner.name == "alice")
        .count();
    assert!(
        owned == 0,
        "Expire must have removed every token owned by User:alice"
    );

    // ── Finalizer-removal PATCH landed on the KafkaUser itself. ───
    let observed = state.take_observed();
    let finalizer_patch = observed.iter().find(|r| {
        r.method() == Method::PATCH
            && r.uri().to_string().contains(&format!("/kafkausers/{USER}"))
            && !r.uri().to_string().contains("/status")
    });
    let patch = finalizer_patch.expect("finalizer-removal PATCH must have been observed");
    let body: serde_json::Value = serde_json::from_slice(patch.body()).unwrap();
    // The patch body clears the finalizer list.
    assert!(
        body["metadata"]["finalizers"] == json!([]),
        "finalizer-removal PATCH must empty the finalizer list"
    );

    // 404 the would-be Secret GET (the operator doesn't issue one in
    // the finalizer arm — owner-references cascade), so the cluster
    // ends up with no Secret because the GC controller deletes it.
    // We can't observe the GC here (no kube-controller-manager in the
    // mock), but the absence of a stray Secret PATCH in the finalizer
    // arm is the operator-side guarantee — nothing was apply-patched
    // beyond the finalizer removal.
    let secret_patches: Vec<String> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains(&format!("/secrets/{USER}"))
        })
        .map(|r| r.uri().to_string())
        .collect();
    // Only the pass-1 Secret PATCH should be present; pass-2 must not
    // re-touch the Secret (owner-ref cascade does the cleanup).
    assert!(
        secret_patches.len() <= 1,
        "finalizer arm must not re-apply the Secret, got patches: {secret_patches:?}",
    );
}
