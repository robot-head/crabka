//! Mocked-client integration tests for the slice-20 `KafkaNodePool`
//! reconciler.
//!
//! Happy-path request sequence on a fresh pool:
//!   1. GET   kafkas/<parent>                  (-> 200 parent Kafka)
//!   2. PATCH statefulsets/<parent>-<pool>     (SSA)
//!   3. GET   statefulsets/<parent>-<pool>     (status read)
//!   4. PATCH kafkanodepools/<pool>/status     (merge)
//!
//! Validation-failure paths short-circuit to step 4 (or skip step 1
//! entirely when the cluster label is missing).

use std::collections::BTreeMap;
use std::sync::Arc;

use crabka_operator::controller::kafka_node_pool::reconcile;
use crabka_operator::crd::{KafkaNodePool, KafkaNodePoolSpec, NodeRole};
use http::{Method, Response};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState, fake_parent_kafka_body, fake_pool_body, fake_sts_body, fixture_ctx,
    json_response, mock_client, not_found_body,
};

fn pool_cr(name: &str, namespace: &str, parent: Option<&str>, replicas: i32) -> KafkaNodePool {
    let mut p = KafkaNodePool::new(
        name,
        KafkaNodePoolSpec {
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            replicas,
            node_id_start: 0,
            image: None,
            resources: None,
        },
    );
    p.metadata.namespace = Some(namespace.into());
    p.metadata.uid = Some("pool-uid".into());
    if let Some(parent_name) = parent {
        let mut labels = BTreeMap::new();
        labels.insert("crabka.io/cluster".into(), parent_name.into());
        p.metadata.labels = Some(labels);
    }
    p
}

/// Happy-path rules: parent Kafka exists, STS apply succeeds, STS status
/// read returns `ready_replicas`, pool status patch echoes the pool.
fn happy_path_rules(
    parent: &str,
    pool: &str,
    namespace: &str,
    ready_replicas: Option<i32>,
) -> Vec<MockRule> {
    let sts_name = format!("{parent}-{pool}");

    vec![
        // 1. GET parent Kafka.
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, namespace)),
        },
        // 2. PATCH statefulset (SSA).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, ready_replicas)),
        },
        // 3. GET statefulset (status read).
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, ready_replicas)),
        },
        // 4. PATCH kafkanodepools/<pool>/status.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool}/status"),
            response: json_response(200, &fake_pool_body(pool, namespace, parent)),
        },
    ]
}

fn build_ctx(
    namespace: &str,
    rules: Vec<MockRule>,
) -> (Arc<crabka_operator::context::Context>, Arc<MockState>) {
    let state = MockState::new(rules);
    let client = mock_client(&state, namespace);
    (Arc::new(fixture_ctx(client, namespace)), state)
}

#[tokio::test]
async fn pool_applies_statefulset_with_pool_name() {
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "brokers", "y", Some(1)));
    let pool = pool_cr("brokers", "y", Some("demo"), 1);

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/statefulsets/"))
        .expect("StatefulSet PATCH must have been captured");
    assert!(
        sts_patch
            .uri()
            .to_string()
            .contains("/statefulsets/demo-brokers"),
        "StatefulSet name should be `<parent>-<pool>` = demo-brokers, got: {}",
        sts_patch.uri(),
    );

    assert_eq!(state.remaining_rules(), 0);
}

#[tokio::test]
async fn pool_status_ready_when_sts_ready() {
    let (ctx, state) = build_ctx("y", happy_path_rules("demo", "brokers", "y", Some(1)));
    let pool = pool_cr("brokers", "y", Some("demo"), 1);

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains("/kafkanodepools/brokers/status")
        })
        .expect("pool status PATCH must have been captured");

    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["type"], "Ready", "body = {body}");
    assert_eq!(cond["status"], "True", "body = {body}");
    assert_eq!(cond["reason"], "Available", "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}

#[tokio::test]
async fn pool_validation_rejects_replicas_two() {
    // Validation runs before any I/O against parent / STS. Only the
    // status patch should fire.
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/kafkanodepools/brokers/status".into(),
        response: json_response(200, &fake_pool_body("brokers", "y", "demo")),
    }];
    let (ctx, state) = build_ctx("y", rules);
    let pool = pool_cr("brokers", "y", Some("demo"), 2);

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    for req in &observed {
        let uri = req.uri().to_string();
        assert!(
            !uri.contains("/statefulsets/"),
            "validation must not touch statefulsets: {uri}",
        );
        assert!(
            !uri.contains("/kafkas/demo"),
            "validation must not look up the parent Kafka: {uri}",
        );
    }
    assert_eq!(
        observed.len(),
        1,
        "validation path should issue exactly one request, saw: {:?}",
        observed
            .iter()
            .map(|r| (r.method().clone(), r.uri().to_string()))
            .collect::<Vec<_>>(),
    );

    let status_patch = &observed[0];
    assert_eq!(status_patch.method(), Method::PATCH);
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["type"], "Ready");
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "UnsupportedReplicaCount");

    assert_eq!(state.remaining_rules(), 0);
}

#[tokio::test]
async fn pool_validation_rejects_missing_cluster_label() {
    // With no `crabka.io/cluster` label, validation passes (label is
    // checked separately) but the parent lookup short-circuits via the
    // `PoolMissingClusterLabel` error. Slice-20 design surfaces that as
    // a `Ready=False / MissingClusterLabel` condition without any
    // parent / STS I/O.
    //
    // The reconciler currently raises `ReconcileError::PoolMissingClusterLabel`
    // before any I/O when no label is present, so no requests are
    // observed. Assert that.
    let (ctx, state) = build_ctx("y", vec![]);
    let pool = pool_cr("brokers", "y", None, 1);

    let res = reconcile(Arc::new(pool), ctx).await;
    assert!(
        res.is_err(),
        "expected reconcile to surface PoolMissingClusterLabel as an error",
    );

    let observed = state.take_observed();
    for req in &observed {
        let uri = req.uri().to_string();
        assert!(
            !uri.contains("/kafkas/"),
            "missing-label path must not look up the parent Kafka: {uri}",
        );
        assert!(
            !uri.contains("/statefulsets/"),
            "missing-label path must not touch statefulsets: {uri}",
        );
    }
}

#[tokio::test]
async fn pool_status_parent_not_found() {
    let rules = vec![
        // 1. GET kafkas/<parent> -> 404
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/demo".into(),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("kafka not found"))
                .expect("404 builds"),
        },
        // 2. PATCH kafkanodepools/<pool>/status with ParentNotFound.
        MockRule {
            method: Method::PATCH,
            path_substr: "/kafkanodepools/brokers/status".into(),
            response: json_response(200, &fake_pool_body("brokers", "y", "demo")),
        },
    ];
    let (ctx, state) = build_ctx("y", rules);
    let pool = pool_cr("brokers", "y", Some("demo"), 1);

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    for req in &observed {
        let uri = req.uri().to_string();
        assert!(
            !uri.contains("/statefulsets/"),
            "ParentNotFound path must not touch statefulsets: {uri}",
        );
    }

    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains("/kafkanodepools/brokers/status")
        })
        .expect("pool status PATCH must have been captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["type"], "Ready", "body = {body}");
    assert_eq!(cond["status"], "False", "body = {body}");
    assert_eq!(cond["reason"], "ParentNotFound", "body = {body}");

    assert_eq!(state.remaining_rules(), 0);
}
