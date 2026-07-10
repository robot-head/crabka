//! Reconcile-level tests for the `KafkaRebalance` controller.
//!
//! Drive the controller's annotation-driven state machine against a faked
//! `crabka-rebalancer` (the `FakeRebalancerClient`) and assert both the
//! Connect-RPC sequence and the kube-side status / annotation patches.

use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;
use crabka_operator::{
    controller::rebalance::reconcile,
    crd::{KafkaCondition, KafkaRebalance, KafkaRebalanceSpec, KafkaRebalanceStatus},
    rebalancer_client::ProposalStatus,
};
use http::{Method, Request};
use hyper::body::Bytes;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, build_ctx, fake_rebalance_body,
    fake_rebalancer::{FakeRebalancerClient, FakeResp, RebalCall, fake_proposal},
    json_response,
};

const NS: &str = "kafka";
const ENDPOINT: &str = "http://r-rebalancer.kafka.svc.cluster.local:9300";

fn rebalance(name: &str) -> KafkaRebalance {
    let mut kr = KafkaRebalance::new(
        name,
        KafkaRebalanceSpec {
            endpoint: Some(ENDPOINT.into()),
            ..Default::default()
        },
    );
    kr.metadata.namespace = Some(NS.into());
    kr.metadata.uid = Some("rebalance-uid".into());
    kr.metadata.generation = Some(1);
    kr
}

fn cond(type_: &str) -> KafkaCondition {
    KafkaCondition {
        type_: type_.into(),
        status: "True".into(),
        reason: type_.into(),
        message: String::new(),
        last_transition_time: "2026-05-22T00:00:00Z".into(),
    }
}

fn with_state(mut kr: KafkaRebalance, state: &str, session: Option<&str>) -> KafkaRebalance {
    kr.status = Some(KafkaRebalanceStatus {
        conditions: vec![cond(state)],
        session_id: session.map(str::to_string),
        ..Default::default()
    });
    kr
}

fn annotate(mut kr: KafkaRebalance, command: &str) -> KafkaRebalance {
    let mut a = BTreeMap::new();
    a.insert("crabka.io/rebalance".to_string(), command.to_string());
    kr.metadata.annotations = Some(a);
    kr
}

fn status_rule(name: &str) -> MockRule {
    MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkarebalances/{name}/status"),
        response: json_response(200, &fake_rebalance_body(name, NS)),
    }
}

fn annotation_rule(name: &str) -> MockRule {
    MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkarebalances/{name}?"),
        response: json_response(200, &fake_rebalance_body(name, NS)),
    }
}

fn status_patch_body(observed: &[Request<Bytes>], name: &str) -> serde_json::Value {
    let suffix = format!("/kafkarebalances/{name}/status");
    let req = observed
        .iter()
        .find(|r| r.uri().to_string().contains(&suffix))
        .expect("status PATCH must have been captured");
    serde_json::from_slice(req.body()).expect("status body is JSON")
}

/// New rebalance with no status → `CreateProposal` → `ProposalReady`,
/// recording the goals and the returned session id.
#[tokio::test]
async fn new_rebalance_creates_proposal() {
    let (ctx, state) = build_ctx(NS, vec![status_rule("demo")]);
    let fake = Arc::new(
        FakeRebalancerClient::new().with_create(FakeResp::Ok(fake_proposal(
            "p-new",
            ProposalStatus::Computed,
        ))),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let mut kr = rebalance("demo");
    kr.spec.goals = Some(vec!["RackAware".into()]);
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(fake.calls() == vec![RebalCall::CreateProposal(vec!["RackAware".into()])]);

    let body = status_patch_body(&state.take_observed(), "demo");
    assert_eq!(
        body["status"]["conditions"][0]["type"].as_str(),
        Some("ProposalReady")
    );
    assert_eq!(body["status"]["sessionId"].as_str(), Some("p-new"));
    assert_eq!(
        body["status"]["optimizationResult"]["replicaMovements"].as_i64(),
        Some(2)
    );
    assert_eq!(body["status"]["observedGeneration"].as_i64(), Some(1));
}

/// `approve` on a `ProposalReady` proposal → `ExecuteProposal` (with the
/// configured throttle) → `Rebalancing`, and the annotation is consumed.
#[tokio::test]
async fn approve_executes_and_enters_rebalancing() {
    let (ctx, state) = build_ctx(NS, vec![annotation_rule("demo"), status_rule("demo")]);
    let fake = Arc::new(
        FakeRebalancerClient::new()
            .with_execute(FakeResp::Ok(fake_proposal("p1", ProposalStatus::Executing))),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let mut kr = with_state(rebalance("demo"), "ProposalReady", Some("p1"));
    kr.spec.throttle_bytes_per_sec = Some(52_428_800);
    let kr = annotate(kr, "approve");
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(
        fake.calls()
            == vec![RebalCall::ExecuteProposal {
                id: "p1".into(),
                throttle: Some(52_428_800),
            }]
    );

    let observed = state.take_observed();
    // Annotation consumed via a merge-null patch on the object (not /status).
    let annotation_patch = observed
        .iter()
        .find(|r| {
            let u = r.uri().to_string();
            u.contains("/kafkarebalances/demo?") && r.method() == Method::PATCH
        })
        .expect("annotation removal PATCH must have been captured");
    let ann_body: serde_json::Value = serde_json::from_slice(annotation_patch.body()).unwrap();
    assert!(
        ann_body["metadata"]["annotations"]["crabka.io/rebalance"].is_null(),
        "expected annotation merge-null, got {ann_body}"
    );

    let body = status_patch_body(&observed, "demo");
    assert_eq!(
        body["status"]["conditions"][0]["type"].as_str(),
        Some("Rebalancing")
    );
    assert_eq!(body["status"]["sessionId"].as_str(), Some("p1"));
}

/// Polling an in-flight execution that has completed → `Ready`.
#[tokio::test]
async fn poll_completes_to_ready() {
    let (ctx, state) = build_ctx(NS, vec![status_rule("demo")]);
    let fake = Arc::new(
        FakeRebalancerClient::new()
            .with_get(FakeResp::Ok(fake_proposal("p1", ProposalStatus::Completed))),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let kr = with_state(rebalance("demo"), "Rebalancing", Some("p1"));
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(fake.calls() == vec![RebalCall::GetProposal("p1".into())]);
    let body = status_patch_body(&state.take_observed(), "demo");
    assert_eq!(
        body["status"]["conditions"][0]["type"].as_str(),
        Some("Ready")
    );
    assert_eq!(body["status"]["sessionId"].as_str(), Some("p1"));
}

/// `stop` while `Rebalancing` → `CancelExecution` → `Stopped`.
#[tokio::test]
async fn stop_cancels_to_stopped() {
    let (ctx, state) = build_ctx(NS, vec![annotation_rule("demo"), status_rule("demo")]);
    let fake = Arc::new(
        FakeRebalancerClient::new()
            .with_cancel(FakeResp::Ok(fake_proposal("p1", ProposalStatus::Cancelled))),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let kr = annotate(
        with_state(rebalance("demo"), "Rebalancing", Some("p1")),
        "stop",
    );
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(fake.calls() == vec![RebalCall::CancelExecution("p1".into())]);
    let body = status_patch_body(&state.take_observed(), "demo");
    assert!(body["status"]["conditions"][0]["type"] == "Stopped");
}

/// A failed execution surfaces `NotReady` with the broker's reason.
#[tokio::test]
async fn poll_failure_surfaces_not_ready() {
    let (ctx, state) = build_ctx(NS, vec![status_rule("demo")]);
    let mut failed = fake_proposal("p1", ProposalStatus::Failed);
    failed.failure_reason = Some("broker 3 unreachable".into());
    let fake = Arc::new(FakeRebalancerClient::new().with_get(FakeResp::Ok(failed)));
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let kr = with_state(rebalance("demo"), "Rebalancing", Some("p1"));
    reconcile(Arc::new(kr), ctx).await.unwrap();

    let body = status_patch_body(&state.take_observed(), "demo");
    assert_eq!(
        body["status"]["conditions"][0]["type"].as_str(),
        Some("NotReady")
    );
    assert_eq!(
        body["status"]["conditions"][0]["message"].as_str(),
        Some("broker 3 unreachable")
    );
}

/// No `spec.endpoint` and no `crabka.io/cluster` label → `NotReady` with
/// `MissingEndpoint` and zero Connect-RPCs.
#[tokio::test]
async fn missing_endpoint_sets_not_ready() {
    let (ctx, state) = build_ctx(NS, vec![status_rule("demo")]);
    // No fake injected — the controller must not reach the client.

    let mut kr = KafkaRebalance::new("demo", KafkaRebalanceSpec::default());
    kr.metadata.namespace = Some(NS.into());
    kr.metadata.uid = Some("rebalance-uid".into());
    reconcile(Arc::new(kr), ctx).await.unwrap();

    let body = status_patch_body(&state.take_observed(), "demo");
    assert_eq!(
        body["status"]["conditions"][0]["type"].as_str(),
        Some("NotReady")
    );
    assert_eq!(
        body["status"]["conditions"][0]["reason"].as_str(),
        Some("MissingEndpoint")
    );
}

/// A transport error leaves the status untouched (no kube writes) so the
/// next reconcile retries — the proposal computation isn't lost to a
/// transient blip.
#[tokio::test]
async fn transport_error_leaves_status_untouched() {
    // Zero rules: any kube call would 404 and surface as an unexpected
    // request. The reconcile must short-circuit before patching.
    let (ctx, state) = build_ctx(NS, vec![]);
    let fake = Arc::new(
        FakeRebalancerClient::new().with_create(FakeResp::Transport("connection refused".into())),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let kr = rebalance("demo");
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(fake.calls() == vec![RebalCall::CreateProposal(vec![])]);
    assert!(
        state.take_observed().is_empty(),
        "transport error must not issue any kube requests"
    );
}
