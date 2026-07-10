//! Reconcile-level integration tests for the delegation-token
//! `KafkaUser` arm.
//!
//! These tests exercise the production dispatch path in
//! `controller::user::reconcile` — same harness as `reconcile_user.rs`
//! (mock kube transport + injected `FakeAdminClient`). The fake's
//! delegation-token RPCs are in-memory KIP-48: `Create` mints a fresh
//! token with `expiry_ts = now + 7d` / `max_ts = now + 30d`; `Renew`
//! advances `expiry_ts` to `min(now + 7d, max_ts)`; `Expire` tombstones
//! the matching entry. The fake's per-call recording lets us assert on
//! the exact RPC sequence the dispatch emits.
//!
//! Why mock rather than a real broker: the operator's per-resource
//! reconcile path is unit-isolated from broker I/O — the
//! `DelegationTokenAdmin` trait gives us a substitution seam. The
//! broker-side act-as wire path is covered by `crabka_broker`'s own
//! integration tests.

use std::{collections::BTreeMap, sync::Arc};

use assert2::check;
use crabka_operator::{
    controller::user::reconcile,
    crd::{Authentication, DelegationTokenAuth, KafkaUser, KafkaUserSpec},
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

/// Echo body for the user-Secret PATCH. The operator only inspects this
/// to confirm the apply succeeded; the test asserts on the request body.
fn secret_body(name: &str, namespace: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "secret-uid" },
        "type": "Opaque",
        "data": {}
    })
}

/// Echo body for the `KafkaUser` status PATCH. kube-rs requires the
/// response deserialize back into a `KafkaUser`, so we echo a minimal
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

/// FIFO mock rules covering one happy-path reconcile of a
/// delegation-token user: Kafka GET → Secret PATCH → status PATCH.
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
    ]
}

// ─── Test 1: first reconcile mints token + writes Secret + status ───────

/// Spec §3.2: a fresh delegation-token user with no existing token
/// results in `Describe → Create → Secret apply → status patch`. The
/// Secret carries the four KIP-48 keys; status carries the token id /
/// expiry, plus `Ready=True` and `TokenIssued=True` conditions.
#[tokio::test]
async fn delegation_token_user_reconcile_creates_secret_and_status() {
    let state = MockState::new(happy_path_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let ku = ku_delegation_token(
        USER,
        DelegationTokenAuth {
            renewers: vec!["User:bob".into()],
            max_lifetime_ms: None,
            renew_before_expiry_ms: None,
        },
    );
    reconcile(Arc::new(ku), ctx).await.unwrap();

    // ── Admin-call shape ────────────────────────────────────────────
    let calls = fake_for_assert.lock().await.calls();
    // Describe (empty) → Create.
    assert2::assert!(calls.iter().any(|c| matches!(
        c,
        RecordedCall::DescribeDelegationTokensOwnedBy { owner_principal }
            if owner_principal == "User:alice"
    )));
    let create_call = calls.iter().find_map(|c| match c {
        RecordedCall::CreateDelegationToken {
            owner_principal_name,
            renewers,
            max_lifetime_ms,
        } => Some((
            owner_principal_name.clone(),
            renewers.clone(),
            *max_lifetime_ms,
        )),
        _ => None,
    });
    // Owner principal name carries no `User:` prefix; unset
    // spec.max_lifetime_ms → -1 (broker default).
    assert2::assert!(
        create_call == Some(("alice".to_string(), vec!["User:bob".to_string()], -1_i64,))
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
        assert2::assert!(data.contains_key(key));
    }

    // ── Status PATCH body: token fields + conditions ───────────────
    let status_patch = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
        .expect("status PATCH must have been observed");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let status = &body["status"];
    let expiry = status["delegationTokenExpiryTimestampMs"]
        .as_i64()
        .expect("delegationTokenExpiryTimestampMs is i64");
    let max_ts = status["delegationTokenMaxTimestampMs"]
        .as_i64()
        .expect("delegationTokenMaxTimestampMs is i64");
    let conds = status["conditions"].as_array().expect("conditions array");
    assert2::assert!(status["delegationTokenId"].is_string());
    assert2::assert!(expiry > 0);
    assert2::assert!(max_ts >= expiry);
    assert2::assert!(
        conds
            .iter()
            .any(|c| c["type"] == "Ready" && c["status"] == "True" && c["reason"] == "TokenReady")
    );
    assert2::assert!(
        conds.iter().any(|c| c["type"] == "TokenIssued"
            && c["status"] == "True"
            && c["reason"] == "Issued")
    );
}

// ─── Test 2: renewal fires when remaining lifetime ≤ threshold ──────────

/// Spec §3.2: with `renew_before_expiry_ms = 7d` and a default 7d token
/// lifetime, `expiry - now <= 7d` is always true → every reconcile
/// fires the Renew path. We reconcile twice, asserting that the second
/// pass calls Renew (not Create) and that the post-renew expiry has
/// advanced (or held at `max_timestamp_ms` if we'd hit the ceiling).
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
        max_lifetime_ms: None,
        renew_before_expiry_ms: Some(7 * 24 * 60 * 60 * 1_000),
    };

    // ── Pass 1: Create + status patch ──────────────────────────────
    let ku = ku_delegation_token(USER, auth.clone());
    reconcile(Arc::new(ku), ctx.clone()).await.unwrap();

    let observed_first = state.take_observed();
    let status_first = observed_first
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
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
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{USER}/status"))
        })
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
    assert2::assert!(renews == 1);
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

/// Spec §3.2: deleting the `KafkaUser` (`deletion_timestamp` set) triggers
/// the finalizer. The finalizer calls `expire_owned_tokens` (which
/// Describe-lists tokens owned by `User:<name>` and Expires each), then
/// removes the finalizer. Owner-references on the Secret then cascade
/// the Secret delete; the test verifies the fake's token store is empty
/// and the finalizer-removal PATCH landed.
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
        assert2::assert!(tokens.len() == 1);
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
    assert2::assert!(describes == 2);
    let expires = calls
        .iter()
        .filter(|c| matches!(c, RecordedCall::ExpireDelegationToken { .. }))
        .count();
    assert2::assert!(expires == 1);

    // ── Token store is now empty (Expire dropped the entry). ──────
    let store = fake_for_assert.lock().await;
    let owned = store
        .delegation_tokens
        .lock()
        .unwrap()
        .iter()
        .filter(|t| t.owner.principal_type == "User" && t.owner.name == "alice")
        .count();
    assert2::assert!(owned == 0);

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
    assert2::assert!(body["metadata"]["finalizers"] == json!([]));

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
    assert2::assert!(secret_patches.len() <= 1);
}
