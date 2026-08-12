//! Mocked-client integration tests for the `KafkaNodePool`
//! reconciler.
//!
//! Happy-path request sequence on a fresh pool:
//!   1. GET   kafkas/<parent>                  (-> 200 parent Kafka)
//!   2. LIST  kafkanodepools                   (topology / bootstrap selection)
//!   3. GET   secrets/<parent>-cluster-id      (bootstrap + per-node identities)
//!   4. GET   statefulsets/<parent>-<pool>     (pre-apply; monotonic-storage check)
//!   5. PATCH statefulsets/<parent>-<pool>     (SSA)
//!   6. GET   statefulsets/<parent>-<pool>     (post-apply status read)
//!   7. PATCH kafkanodepools/<pool>/status     (merge)
//!
//! Validation-failure paths short-circuit to step 5 (or skip step 1
//! entirely when the cluster label is missing). Monotonic-
//! storage failures short-circuit after step 2.

use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;
use crabka_operator::{
    controller::kafka_node_pool::reconcile,
    crd::{KafkaNodePool, KafkaNodePoolSpec, NodeRole},
};
use http::{Method, Response};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState, fake_parent_kafka_body, fake_pool_body, fake_pool_list_body,
    fake_secret_body, fake_sts_body, fake_sts_body_with_storage, fixture_ctx, json_response,
    mock_client, not_found_body,
};

const DIRECTORY_ID: uuid::Uuid = uuid::Uuid::from_u128(1);

fn pool_cr(name: &str, namespace: &str, parent: Option<&str>, replicas: i32) -> KafkaNodePool {
    let mut p = KafkaNodePool::new(
        name,
        KafkaNodePoolSpec {
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            replicas,
            node_id_start: 0,
            image: None,
            resources: None,
            client_dispatch_queue_capacity: None,
            client_frame_max: None,
            template: None,
            storage: None,
        },
    );
    p.metadata.namespace = Some(namespace.into());
    p.metadata.uid = Some("pool-uid".into());
    p.metadata.finalizers = Some(vec!["crabka.io/kafka-node-pool-finalizer".into()]);
    if let Some(parent_name) = parent {
        let mut labels = BTreeMap::new();
        labels.insert("crabka.io/cluster".into(), parent_name.into());
        p.metadata.labels = Some(labels);
    }
    p
}

fn dynamic_secret_body(parent: &str, pool: &str, namespace: &str) -> serde_json::Value {
    dynamic_secret_body_for_ids(parent, pool, namespace, &[(0, DIRECTORY_ID)])
}

fn dynamic_secret_body_for_ids(
    parent: &str,
    pool: &str,
    namespace: &str,
    directory_ids: &[(i32, uuid::Uuid)],
) -> serde_json::Value {
    use base64::Engine as _;

    let mut secret = fake_secret_body(
        &format!("{parent}-cluster-id"),
        namespace,
        "00000000-0000-0000-0000-000000000001",
    );
    let data = secret["data"]
        .as_object_mut()
        .expect("fake Secret data object");
    let encode = |value: &str| {
        serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(value))
    };
    data.insert("quorumBootstrapNodeId".into(), encode("0"));
    data.insert("quorumBootstrapPool".into(), encode(pool));
    data.insert("quorumBootstrapInitialized".into(), encode("true"));
    for (node_id, directory_id) in directory_ids {
        data.insert(
            format!("quorumDirectoryId-{node_id}"),
            encode(&directory_id.to_string()),
        );
    }
    secret
}

fn empty_statefulset_list_rule() -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: "/statefulsets?".into(),
        response: json_response(
            200,
            &serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSetList",
                "metadata": { "resourceVersion": "1" },
                "items": [],
            }),
        ),
    }
}

fn pod_list_rule(namespace: &str, names: &[&str]) -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: "/pods?".into(),
        response: json_response(
            200,
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "PodList",
                "metadata": { "resourceVersion": "1" },
                "items": names
                    .iter()
                    .map(|name| serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": { "name": name, "namespace": namespace },
                    }))
                    .collect::<Vec<_>>(),
            }),
        ),
    }
}

fn dynamic_quorum_rules(parent: &str, pool: &str, namespace: &str) -> Vec<MockRule> {
    let secret = dynamic_secret_body(parent, pool, namespace);
    vec![
        MockRule {
            method: Method::GET,
            path_substr: "/kafkanodepools?".into(),
            response: json_response(
                200,
                &fake_pool_list_body(&[fake_pool_body(pool, namespace, parent)]),
            ),
        },
        empty_statefulset_list_rule(),
        pod_list_rule(namespace, &[]),
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(200, &secret),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(200, &secret),
        },
    ]
}

fn stopped_pool_rules(parent: &str, pool: &str, parent_body: &serde_json::Value) -> Vec<MockRule> {
    vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, parent_body),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{parent}-{pool}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("StatefulSet already stopped"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/pods?".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PodList",
                    "metadata": { "resourceVersion": "1" },
                    "items": [],
                }),
            ),
        },
    ]
}

/// A parent Kafka whose version model has NOT cleared: the Kafka
/// controller published `KafkaVersionValid=False` and finalized no
/// metadata version (the fresh-cluster, invalid-`kafkaVersion` case).
fn fake_parent_kafka_body_version_invalid(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": { "kafkaVersion": "99.9.bogus" },
        "status": {
            "conditions": [{
                "type": "KafkaVersionValid",
                "status": "False",
                "reason": "InvalidVersion",
                "message": "spec.kafkaVersion \"99.9.bogus\" is not a valid version",
                "lastTransitionTime": "2026-05-22T00:00:00Z"
            }]
        }
    })
}

/// Happy-path rules: parent Kafka exists, STS apply succeeds, STS status
/// read returns `ready_replicas`, pool status patch echoes the pool.
///
/// The reconcile flow includes a pre-apply STS GET (for
/// monotonic-storage validation), so the rule sequence is:
///   1. GET parent Kafka.
///   2. GET STS (pre-apply): 404 → first-reconcile, validation accepts any spec.
///   3. PATCH STS (SSA).
///   4. GET STS (post-apply): returns `ready_replicas` for the status mirror.
///   5. PATCH pool status.
fn happy_path_rules(
    parent: &str,
    pool: &str,
    namespace: &str,
    ready_replicas: Option<i32>,
) -> Vec<MockRule> {
    let sts_name = format!("{parent}-{pool}");

    let mut rules = vec![
        // 1. GET parent Kafka.
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, namespace)),
        },
    ];
    rules.extend(dynamic_quorum_rules(parent, pool, namespace));
    rules.extend([
        // 2. GET statefulset (pre-apply, monotonic-storage check):
        //    no live STS on first reconcile.
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("first reconcile, no live STS"))
                .expect("404 builds"),
        },
        // 3. PATCH statefulset (SSA).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, ready_replicas)),
        },
        // 4. GET statefulset (post-apply status read).
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, ready_replicas)),
        },
        // 5. PATCH kafkanodepools/<pool>/status.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool}/status"),
            response: json_response(200, &fake_pool_body(pool, namespace, parent)),
        },
    ]);
    rules
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

    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn multi_replica_pool_reconcile_renders_all_ordinals() {
    let parent = "demo";
    let pool_name = "controllers";
    let namespace = "y";
    let sts_name = format!("{parent}-{pool_name}");
    let directory_ids = [
        (0, DIRECTORY_ID),
        (1, uuid::Uuid::from_u128(2)),
        (2, uuid::Uuid::from_u128(3)),
    ];
    let secret = dynamic_secret_body_for_ids(parent, pool_name, namespace, &directory_ids);
    let mut sibling = fake_pool_body(pool_name, namespace, parent);
    sibling["spec"]["roles"] = serde_json::json!(["Controller"]);
    sibling["spec"]["replicas"] = serde_json::json!(3);
    let mut rules = vec![MockRule {
        method: Method::GET,
        path_substr: format!("/kafkas/{parent}"),
        response: json_response(200, &fake_parent_kafka_body(parent, namespace)),
    }];
    rules.push(MockRule {
        method: Method::GET,
        path_substr: "/kafkanodepools?".into(),
        response: json_response(200, &fake_pool_list_body(&[sibling])),
    });
    rules.push(empty_statefulset_list_rule());
    rules.push(pod_list_rule(namespace, &[]));
    for _ in 0..=3 {
        rules.push(MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(200, &secret),
        });
    }
    rules.extend([
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("first reconcile, no live STS"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 3, None)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 3, None)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, namespace, parent)),
        },
    ]);
    let (ctx, state) = build_ctx(namespace, rules);
    let mut pool = pool_cr(pool_name, namespace, Some(parent), 3);
    pool.spec.roles = vec![NodeRole::Controller];

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let request = state
        .take_observed()
        .into_iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request.uri().to_string().contains("/statefulsets/")
        })
        .expect("StatefulSet patch");
    let body: serde_json::Value = serde_json::from_slice(request.body()).unwrap();
    assert!(body["spec"]["replicas"] == 3);
    assert!(
        body["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array()
            .expect("broker env")
            .iter()
            .any(|env| { env["name"] == "CRABKA_PROCESS_ROLES" && env["value"] == "controller" })
    );
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn controller_scale_down_removes_highest_voter_before_pods() {
    use crabka_client_admin::{MetadataQuorum, QuorumReplica};

    let parent = "demo";
    let pool_name = "controllers";
    let namespace = "y";
    let sts_name = format!("{parent}-{pool_name}");
    let directory_ids = [
        (0, DIRECTORY_ID),
        (1, uuid::Uuid::from_u128(2)),
        (2, uuid::Uuid::from_u128(3)),
        (3, uuid::Uuid::from_u128(4)),
    ];
    let secret = dynamic_secret_body_for_ids(parent, pool_name, namespace, &directory_ids);
    let mut sibling = fake_pool_body(pool_name, namespace, parent);
    sibling["spec"]["roles"] = serde_json::json!(["Controller"]);
    sibling["spec"]["replicas"] = serde_json::json!(2);
    let mut rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, namespace)),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/kafkanodepools?".into(),
            response: json_response(200, &fake_pool_list_body(&[sibling])),
        },
        empty_statefulset_list_rule(),
        pod_list_rule(namespace, &[]),
    ];
    for _ in 0..4 {
        rules.push(MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(200, &secret),
        });
    }
    rules.extend([
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 4, Some(4))),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, namespace, parent)),
        },
    ]);
    let state = MockState::new(rules);
    let ctx = fixture_ctx(mock_client(&state, namespace), namespace);
    let admin = Arc::new(tokio::sync::Mutex::new(
        shared::fake_admin::FakeAdminClient::new(),
    ));
    admin.lock().await.set_metadata_quorum(MetadataQuorum {
        leader_id: 0,
        leader_epoch: 1,
        high_watermark: 1,
        voters: directory_ids
            .into_iter()
            .map(|(node_id, directory_id)| QuorumReplica {
                node_id,
                directory_id,
                log_end_offset: 1,
                last_fetch_timestamp: -1,
                last_caught_up_timestamp: -1,
            })
            .collect(),
        observers: Vec::new(),
    });
    ctx.insert_admin_client_for_test(parent, admin.clone())
        .await;
    let mut pool = pool_cr(pool_name, namespace, Some(parent), 2);
    pool.spec.roles = vec![NodeRole::Controller];

    reconcile(Arc::new(pool), Arc::new(ctx)).await.unwrap();

    let calls = admin.lock().await.calls();
    assert!(matches!(
        calls.as_slice(),
        [
            shared::fake_admin::RecordedCall::DescribeMetadataQuorum,
            shared::fake_admin::RecordedCall::RemoveRaftVoter {
                node_id: 3,
                directory_id,
                ..
            }
        ] if *directory_id == uuid::Uuid::from_u128(4)
    ));
    let observed = state.take_observed();
    assert!(observed.iter().all(|request| {
        !(request.method() == Method::PATCH && request.uri().to_string().contains("/statefulsets/"))
    }));
    let status = observed
        .iter()
        .find(|request| request.uri().to_string().contains("/status"))
        .expect("scale-down status patch");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["conditions"][0]["reason"] == "QuorumScaleDownInProgress");
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn concurrent_shrink_and_add_cannot_reuse_live_statefulset_node_ids() {
    let parent = "demo";
    let namespace = "y";
    let mut shrinking = fake_pool_body("controllers", namespace, parent);
    shrinking["spec"]["roles"] = serde_json::json!(["Controller"]);
    shrinking["spec"]["replicas"] = serde_json::json!(1);
    let mut added = fake_pool_body("brokers", namespace, parent);
    added["spec"]["roles"] = serde_json::json!(["Broker"]);
    added["spec"]["nodeIdStart"] = serde_json::json!(2);
    let mut live = fake_sts_body("demo-controllers", namespace, 1, Some(1));
    live["metadata"]["labels"] = serde_json::json!({
        "app.kubernetes.io/instance": parent,
        "app.kubernetes.io/name": "crabka-broker",
        "crabka.io/pool": "controllers",
    });
    live["metadata"]["annotations"] = serde_json::json!({
        "crabka.io/node-id-start": "0",
        "crabka.io/process-roles": "controller",
    });
    live["status"]["replicas"] = serde_json::json!(3);
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, namespace)),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/kafkanodepools?".into(),
            response: json_response(200, &fake_pool_list_body(&[shrinking, added])),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/statefulsets?".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "StatefulSetList",
                    "metadata": { "resourceVersion": "1" },
                    "items": [live],
                }),
            ),
        },
        pod_list_rule(namespace, &[]),
        MockRule {
            method: Method::PATCH,
            path_substr: "/kafkanodepools/brokers/status".into(),
            response: json_response(200, &fake_pool_body("brokers", namespace, parent)),
        },
    ];
    let (ctx, state) = build_ctx(namespace, rules);
    let mut pool = pool_cr("brokers", namespace, Some(parent), 1);
    pool.spec.roles = vec![NodeRole::Broker];
    pool.spec.node_id_start = 2;

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let status = observed
        .iter()
        .find(|request| request.uri().to_string().contains("/status"))
        .expect("topology status patch");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["conditions"][0]["reason"] == "NodeIdRangeOverlap");
    assert!(observed.iter().all(|request| {
        request.method() != Method::PATCH || !request.uri().to_string().contains("/statefulsets/")
    }));
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn broker_only_pool_waits_for_controller_pool() {
    let parent = "demo";
    let namespace = "y";
    let mut controller = fake_pool_body("controllers", namespace, parent);
    controller["spec"]["roles"] = serde_json::json!(["Controller"]);
    controller["spec"]["nodeIdStart"] = serde_json::json!(10);
    let mut broker = fake_pool_body("brokers", namespace, parent);
    broker["spec"]["roles"] = serde_json::json!(["Broker"]);
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, namespace)),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/kafkanodepools?".into(),
            response: json_response(200, &fake_pool_list_body(&[controller, broker])),
        },
        empty_statefulset_list_rule(),
        pod_list_rule(namespace, &[]),
        MockRule {
            method: Method::PATCH,
            path_substr: "/kafkanodepools/brokers/status".into(),
            response: json_response(200, &fake_pool_body("brokers", namespace, parent)),
        },
    ];
    let (ctx, state) = build_ctx(namespace, rules);
    let mut pool = pool_cr("brokers", namespace, Some(parent), 1);
    pool.spec.roles = vec![NodeRole::Broker];

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let status = observed
        .iter()
        .find(|request| request.uri().to_string().contains("/status"))
        .expect("status patch");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["conditions"][0]["reason"] == "WaitingForControllers");
    assert!(
        observed
            .iter()
            .all(|request| request.method() != Method::PATCH
                || !request.uri().to_string().contains("/statefulsets/"))
    );
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn broker_only_pool_becomes_ready_without_joining_quorum() {
    let parent = "demo";
    let namespace = "y";
    let pool_name = "brokers";
    let sts_name = format!("{parent}-{pool_name}");
    let secret =
        dynamic_secret_body_for_ids(parent, "controllers", namespace, &[(10, DIRECTORY_ID)]);
    let mut controller = fake_pool_body("controllers", namespace, parent);
    controller["spec"]["roles"] = serde_json::json!(["Controller"]);
    controller["status"]["replicas"] = serde_json::json!(1);
    controller["status"]["readyReplicas"] = serde_json::json!(1);
    controller["status"]["conditions"] = serde_json::json!([{
        "type": "Ready",
        "status": "True",
        "reason": "Available",
        "message": "controller quorum member is ready",
        "lastTransitionTime": "2026-08-11T00:00:00Z"
    }]);
    let mut broker = fake_pool_body(pool_name, namespace, parent);
    broker["spec"]["roles"] = serde_json::json!(["Broker"]);
    broker["spec"]["nodeIdStart"] = serde_json::json!(10);
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, namespace)),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/kafkanodepools?".into(),
            response: json_response(200, &fake_pool_list_body(&[controller, broker])),
        },
        empty_statefulset_list_rule(),
        pod_list_rule(namespace, &[]),
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(200, &secret),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(200, &secret),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("first reconcile, no live STS"))
                .expect("404 builds"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, Some(1))),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, namespace, 1, Some(1))),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, namespace, parent)),
        },
    ];
    let (ctx, state) = build_ctx(namespace, rules);
    let mut pool = pool_cr(pool_name, namespace, Some(parent), 1);
    pool.spec.roles = vec![NodeRole::Broker];
    pool.spec.node_id_start = 10;

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request.uri().to_string().contains("/statefulsets/")
        })
        .expect("broker StatefulSet patch");
    let body: serde_json::Value = serde_json::from_slice(sts_patch.body()).unwrap();
    assert!(
        body["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array()
            .expect("broker env")
            .iter()
            .any(|env| { env["name"] == "CRABKA_PROCESS_ROLES" && env["value"] == "broker" })
    );
    let status = observed
        .iter()
        .find(|request| request.uri().to_string().contains("/status"))
        .expect("ready status patch");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["conditions"][0]["status"] == "True");
    assert!(body["status"]["conditions"][0]["reason"] == "Available");
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn pool_status_ready_when_sts_ready() {
    use crabka_client_admin::{MetadataQuorum, QuorumReplica};

    let state = MockState::new(happy_path_rules("demo", "brokers", "y", Some(1)));
    let mut ctx = fixture_ctx(mock_client(&state, "y"), "y");
    Arc::get_mut(&mut ctx.config)
        .expect("fixture owns operator config")
        .controller_dependency_requeue = crabka_units::millis(1_234);
    let admin = shared::fake_admin::FakeAdminClient::new();
    admin.set_metadata_quorum(MetadataQuorum {
        leader_id: 0,
        leader_epoch: 1,
        high_watermark: 1,
        voters: vec![QuorumReplica {
            node_id: 0,
            directory_id: DIRECTORY_ID,
            log_end_offset: 1,
            last_fetch_timestamp: -1,
            last_caught_up_timestamp: -1,
        }],
        observers: Vec::new(),
    });
    ctx.insert_admin_client_for_test("demo", Arc::new(tokio::sync::Mutex::new(admin)))
        .await;
    let pool = pool_cr("brokers", "y", Some("demo"), 1);

    let action = reconcile(Arc::new(pool), Arc::new(ctx)).await.unwrap();
    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(1_234))
    );

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
    for (field, want) in [
        ("type", "Ready"),
        ("status", "True"),
        ("reason", "Available"),
    ] {
        assert!(cond[field] == want, "field {field}; body = {body}");
    }

    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn deleting_pool_removes_exact_committed_voter() {
    use crabka_client_admin::{MetadataQuorum, QuorumReplica};

    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{parent}-{pool_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("StatefulSet absent"))
                .expect("404 builds"),
        },
        pod_list_rule(ns, &[]),
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(200, &dynamic_secret_body(parent, pool_name, ns)),
        },
    ];
    let state = MockState::new(rules);
    let ctx = fixture_ctx(mock_client(&state, ns), ns);
    let admin = Arc::new(tokio::sync::Mutex::new(
        shared::fake_admin::FakeAdminClient::new(),
    ));
    admin.lock().await.set_metadata_quorum(MetadataQuorum {
        leader_id: 1,
        leader_epoch: 3,
        high_watermark: 7,
        voters: vec![
            QuorumReplica {
                node_id: 0,
                directory_id: DIRECTORY_ID,
                log_end_offset: 7,
                last_fetch_timestamp: -1,
                last_caught_up_timestamp: -1,
            },
            QuorumReplica {
                node_id: 1,
                directory_id: uuid::Uuid::from_u128(2),
                log_end_offset: 7,
                last_fetch_timestamp: -1,
                last_caught_up_timestamp: -1,
            },
        ],
        observers: Vec::new(),
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        "2026-08-08T00:00:00Z".parse().unwrap(),
    ));

    reconcile(Arc::new(pool), Arc::new(ctx)).await.unwrap();

    let calls = admin.lock().await.calls();
    assert!(matches!(
        calls.as_slice(),
        [
            shared::fake_admin::RecordedCall::DescribeMetadataQuorum,
            shared::fake_admin::RecordedCall::RemoveRaftVoter {
                node_id: 0,
                directory_id: DIRECTORY_ID,
                ..
            }
        ]
    ));
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn deleting_pool_finishes_observed_downscale_voters_before_pods() {
    use crabka_client_admin::{MetadataQuorum, QuorumReplica};

    let parent = "demo";
    let pool_name = "controllers";
    let ns = "y";
    let directory_ids = [
        (0, DIRECTORY_ID),
        (1, uuid::Uuid::from_u128(2)),
        (2, uuid::Uuid::from_u128(3)),
        (10, uuid::Uuid::from_u128(11)),
    ];
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{parent}-{pool_name}"),
            response: json_response(
                200,
                &fake_sts_body(&format!("{parent}-{pool_name}"), ns, 1, Some(1)),
            ),
        },
        pod_list_rule(
            ns,
            &[
                "demo-controllers-0",
                "demo-controllers-1",
                "demo-controllers-2",
            ],
        ),
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(
                200,
                &dynamic_secret_body_for_ids(parent, pool_name, ns, &directory_ids),
            ),
        },
    ];
    let state = MockState::new(rules);
    let ctx = fixture_ctx(mock_client(&state, ns), ns);
    let admin = Arc::new(tokio::sync::Mutex::new(
        shared::fake_admin::FakeAdminClient::new(),
    ));
    admin.lock().await.set_metadata_quorum(MetadataQuorum {
        leader_id: 0,
        leader_epoch: 3,
        high_watermark: 7,
        voters: directory_ids
            .into_iter()
            .map(|(node_id, directory_id)| QuorumReplica {
                node_id,
                directory_id,
                log_end_offset: 7,
                last_fetch_timestamp: -1,
                last_caught_up_timestamp: -1,
            })
            .collect(),
        observers: Vec::new(),
    });
    ctx.insert_admin_client_for_test(parent, admin.clone())
        .await;
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.spec.roles = vec![NodeRole::Controller];
    pool.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        "2026-08-08T00:00:00Z".parse().unwrap(),
    ));

    reconcile(Arc::new(pool), Arc::new(ctx)).await.unwrap();

    let calls = admin.lock().await.calls();
    assert!(matches!(
        calls.as_slice(),
        [
            shared::fake_admin::RecordedCall::DescribeMetadataQuorum,
            shared::fake_admin::RecordedCall::RemoveRaftVoter {
                node_id: 2,
                directory_id,
                ..
            }
        ] if *directory_id == uuid::Uuid::from_u128(3)
    ));
    let observed = state.take_observed();
    assert!(observed.iter().all(|request| {
        !(request.method() == Method::PATCH
            && (request.uri().to_string().contains("/statefulsets/")
                || request
                    .uri()
                    .to_string()
                    .contains(&format!("/kafkanodepools/{pool_name}"))))
    }));
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn deleting_last_voter_keeps_finalizer_and_reports_blocked() {
    use crabka_client_admin::{MetadataQuorum, QuorumReplica};

    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{parent}-{pool_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("StatefulSet absent"))
                .expect("404 builds"),
        },
        pod_list_rule(ns, &[]),
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{parent}-cluster-id"),
            response: json_response(200, &dynamic_secret_body(parent, pool_name, ns)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ];
    let state = MockState::new(rules);
    let ctx = fixture_ctx(mock_client(&state, ns), ns);
    let admin = shared::fake_admin::FakeAdminClient::new();
    admin.set_metadata_quorum(MetadataQuorum {
        leader_id: 0,
        leader_epoch: 3,
        high_watermark: 7,
        voters: vec![QuorumReplica {
            node_id: 0,
            directory_id: DIRECTORY_ID,
            log_end_offset: 7,
            last_fetch_timestamp: -1,
            last_caught_up_timestamp: -1,
        }],
        observers: Vec::new(),
    });
    ctx.insert_admin_client_for_test("demo", Arc::new(tokio::sync::Mutex::new(admin)))
        .await;
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        "2026-08-08T00:00:00Z".parse().unwrap(),
    ));

    reconcile(Arc::new(pool), Arc::new(ctx)).await.unwrap();

    let status = state
        .take_observed()
        .into_iter()
        .find(|request| request.uri().to_string().contains("/status"))
        .expect("blocked status patch");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["conditions"][0]["reason"] == "LastVoterDeletionBlocked");
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn parent_deletion_releases_pool_finalizer_without_dismantling_quorum() {
    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let mut parent_body = fake_parent_kafka_body(parent, ns);
    parent_body["metadata"]["deletionTimestamp"] =
        serde_json::Value::String("2026-08-08T00:00:00Z".into());
    let mut rules = stopped_pool_rules(parent, pool_name, &parent_body);
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkanodepools/{pool_name}"),
        response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
    });
    let (ctx, state) = build_ctx(ns, rules);
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        "2026-08-08T00:00:00Z".parse().unwrap(),
    ));

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let finalizer_patch = observed
        .iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains(&format!("/kafkanodepools/{pool_name}"))
        })
        .expect("finalizer patch");
    let body: serde_json::Value = serde_json::from_slice(finalizer_patch.body()).unwrap();
    assert!(body["metadata"]["finalizers"] == serde_json::json!([]));
    assert!(
        observed
            .iter()
            .all(|request| !request.uri().to_string().contains("/secrets/"))
    );
    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn pool_validation_rejects_zero_replicas() {
    // Validation runs before any I/O against parent / STS. Only the
    // status patch should fire.
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/kafkanodepools/brokers/status".into(),
        response: json_response(200, &fake_pool_body("brokers", "y", "demo")),
    }];
    let (ctx, state) = build_ctx("y", rules);
    let pool = pool_cr("brokers", "y", Some("demo"), 0);

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
    assert!(
        observed.len() == 1,
        "validation path should issue exactly one request, saw: {:?}",
        observed
            .iter()
            .map(|r| (r.method().clone(), r.uri().to_string()))
            .collect::<Vec<_>>()
    );

    let status_patch = &observed[0];
    assert!(status_patch.method() == Method::PATCH);
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    for (field, want) in [
        ("type", "Ready"),
        ("status", "False"),
        ("reason", "InvalidReplicaCount"),
    ] {
        assert!(cond[field] == want, "field {field}; body = {body}");
    }

    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn pool_validation_rejects_missing_cluster_label() {
    // With no `crabka.io/cluster` label, validation passes (label is
    // checked separately) but the parent lookup short-circuits via the
    // `PoolMissingClusterLabel` error, surfaced as
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
    for (field, want) in [
        ("type", "Ready"),
        ("status", "False"),
        ("reason", "ParentNotFound"),
    ] {
        assert!(cond[field] == want, "field {field}; body = {body}");
    }

    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn pool_persistent_claim_renders_volume_claim_template() {
    use crabka_operator::crd::{PersistentClaimSpec, Storage};

    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let sts_name = format!("{parent}-{pool_name}");

    let mut rules = vec![
        // 1. GET parent Kafka.
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        // 2. Pre-apply GET: no live STS (first reconcile).
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("first reconcile, no live STS"))
                .expect("404 builds"),
        },
        // 3. PATCH STS (SSA-apply).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, ns, 1, Some(1))),
        },
        // 4. Post-apply GET (status read).
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, ns, 1, Some(1))),
        },
        // 5. PATCH pool status.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ];
    rules.extend(dynamic_quorum_rules(parent, pool_name, ns));

    let (ctx, state) = build_ctx(ns, rules);
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "10Gi".into(),
        class: Some("fast-ssd".into()),
        delete_claim: false,
    }));

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/statefulsets/{sts_name}"))
        })
        .expect("STS PATCH was captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS PATCH body is JSON");

    // volumeClaimTemplates carries our data PVC at the requested size +
    // accessModes + storageClassName.
    let vct = body["spec"]["volumeClaimTemplates"]
        .as_array()
        .unwrap_or_else(|| panic!("volumeClaimTemplates present; body = {body}"));
    assert!(vct.len() == 1, "body = {body}");
    let pvc = &vct[0];
    assert!(pvc["metadata"]["name"] == "data", "body = {body}");
    assert!(
        pvc["spec"]
            == serde_json::json!({
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": "10Gi" } },
                "storageClassName": "fast-ssd"
            }),
        "body = {body}"
    );

    // No emptyDir for `data` in the pod-template volumes (the
    // StatefulSet controller mounts the PVC under the same name).
    let volumes = body["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for v in &volumes {
        if v["name"] == "data" {
            assert!(
                v.get("emptyDir").is_none(),
                "expected no emptyDir entry for data; got {v}",
            );
        }
    }

    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn pool_storage_shrink_is_rejected() {
    use crabka_operator::crd::{PersistentClaimSpec, Storage};

    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let sts_name = format!("{parent}-{pool_name}");

    let mut rules = vec![
        // 1. GET parent Kafka.
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        // 2. Pre-apply GET: live STS has volumeClaimTemplates with 10Gi.
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(
                200,
                &fake_sts_body_with_storage(&sts_name, ns, 1, Some(1), Some(("10Gi", None))),
            ),
        },
        // 3. Validation rejects the shrink; status PATCH is the only
        //    request that follows. No STS PATCH, no second STS GET.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ];
    rules.extend(dynamic_quorum_rules(parent, pool_name, ns));

    let (ctx, state) = build_ctx(ns, rules);
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
        size: "5Gi".into(),
        class: None,
        delete_claim: false,
    }));

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    // Assert NO STS PATCH was attempted (the SSA-apply is short-circuited
    // by the monotonic-storage validator).
    for req in &observed {
        let uri = req.uri().to_string();
        if req.method() == Method::PATCH {
            assert!(
                !uri.contains(&format!("/statefulsets/{sts_name}")),
                "shrink path must not PATCH the StatefulSet: {uri}",
            );
        }
    }
    // Status PATCH body has reason=StorageImmutable.
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkanodepools/{pool_name}/status"))
        })
        .expect("status PATCH must be captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    for (field, want) in [
        ("type", "Ready"),
        ("status", "False"),
        ("reason", "StorageImmutable"),
    ] {
        assert!(cond[field] == want, "field {field}; body = {body}");
    }

    assert!(state.remaining_rules() == 0);
}

/// A JBOD pool renders one `volumeClaimTemplate` per disk
/// (`data` + `data-{id}`), a set-wide retention policy, and the broker
/// container's `CRABKA_EXTRA_LOG_DIRS` env listing every non-primary disk.
#[tokio::test]
async fn pool_jbod_renders_multiple_volume_claim_templates() {
    use crabka_operator::crd::{JbodSpec, JbodVolume, Storage};

    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let sts_name = format!("{parent}-{pool_name}");

    let mut rules = vec![
        // 1. GET parent Kafka.
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        // 2. Pre-apply GET: no live STS (first reconcile).
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("first reconcile, no live STS"))
                .expect("404 builds"),
        },
        // 3. PATCH STS (SSA-apply).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, ns, 1, Some(1))),
        },
        // 4. Post-apply GET (status read).
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, ns, 1, Some(1))),
        },
        // 5. PATCH pool status.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ];
    rules.extend(dynamic_quorum_rules(parent, pool_name, ns));

    let (ctx, state) = build_ctx(ns, rules);
    let mut pool = pool_cr(pool_name, ns, Some(parent), 1);
    pool.spec.storage = Some(Storage::Jbod(JbodSpec {
        volumes: vec![
            JbodVolume {
                id: 0,
                size: "1Gi".into(),
                class: None,
            },
            JbodVolume {
                id: 1,
                size: "2Gi".into(),
                class: Some("fast".into()),
            },
        ],
        delete_claim: true,
    }));

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/statefulsets/{sts_name}"))
        })
        .expect("STS PATCH was captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS PATCH body is JSON");

    // One PVC template per disk: primary `data` + `data-1`.
    let vct = body["spec"]["volumeClaimTemplates"]
        .as_array()
        .unwrap_or_else(|| panic!("volumeClaimTemplates present; body = {body}"));
    assert!(vct.len() == 2, "body = {body}");
    let want_templates = [
        (
            "data",
            serde_json::json!({
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": "1Gi" } }
            }),
        ),
        (
            "data-1",
            serde_json::json!({
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": "2Gi" } },
                "storageClassName": "fast"
            }),
        ),
    ];
    for (i, (want_name, want_spec)) in want_templates.iter().enumerate() {
        assert!(
            vct[i]["metadata"]["name"] == *want_name,
            "disk {i}; body = {body}"
        );
        assert!(vct[i]["spec"] == *want_spec, "disk {i}; body = {body}");
    }

    // Set-wide retention honors the JBOD-level deleteClaim.
    assert!(
        body["spec"]["persistentVolumeClaimRetentionPolicy"]
            == serde_json::json!({ "whenDeleted": "Delete", "whenScaled": "Retain" }),
        "body = {body}"
    );

    // Broker container learns the extra disk via CRABKA_EXTRA_LOG_DIRS.
    let containers = body["spec"]["template"]["spec"]["containers"]
        .as_array()
        .unwrap_or_else(|| panic!("containers present; body = {body}"));
    let env = containers[0]["env"]
        .as_array()
        .unwrap_or_else(|| panic!("broker env present; body = {body}"));
    let extra = env
        .iter()
        .find(|e| e["name"] == "CRABKA_EXTRA_LOG_DIRS")
        .unwrap_or_else(|| panic!("CRABKA_EXTRA_LOG_DIRS env present; body = {body}"));
    assert!(extra["value"] == "/var/lib/crabka/data-1", "body = {body}");

    assert!(state.remaining_rules() == 0);
}

/// The rendered `StatefulSet` must:
///   1. Include a `broker-config` `ConfigMap` volume in the pod template.
///   2. Pass `--config-file=/run/crabka/broker.toml` in the broker container args.
///   3. Mount the `ConfigMap` at `/etc/crabka/config` (readOnly) in the broker container.
///   4. NOT include `CRABKA_ADVERTISED_LISTENER` in the broker container env.
#[tokio::test]
async fn statefulset_mounts_broker_config_volume_and_uses_config_file() {
    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";
    let sts_name = format!("{parent}-{pool_name}");

    let mut rules = vec![
        // 1. GET parent Kafka.
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, ns)),
        },
        // 2. Pre-apply GET: no live STS (first reconcile).
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: shared::json_response(
                404,
                &serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "code": 404,
                    "reason": "NotFound",
                    "message": "not found"
                }),
            ),
        },
        // 3. PATCH STS (SSA-apply).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, ns, 1, Some(1))),
        },
        // 4. Post-apply GET (status read).
        MockRule {
            method: Method::GET,
            path_substr: format!("/statefulsets/{sts_name}"),
            response: json_response(200, &fake_sts_body(&sts_name, ns, 1, Some(1))),
        },
        // 5. PATCH pool status.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ];
    rules.extend(dynamic_quorum_rules(parent, pool_name, ns));

    let (ctx, state) = build_ctx(ns, rules);
    let pool = pool_cr(pool_name, ns, Some(parent), 1);

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    let sts_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/statefulsets/{sts_name}"))
        })
        .expect("STS PATCH was captured");
    let body: serde_json::Value =
        serde_json::from_slice(sts_patch.body()).expect("STS PATCH body is JSON");

    // 1. Pod template volumes must include a broker-config ConfigMap volume.
    let volumes = body["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap_or_else(|| panic!("volumes present; body = {body}"));
    let broker_config_vol = volumes
        .iter()
        .find(|v| v["name"] == "broker-config")
        .unwrap_or_else(|| panic!("broker-config volume missing; volumes = {volumes:?}"));
    assert!(
        broker_config_vol["configMap"]["name"] == "demo-broker-config",
        "broker-config volume must reference <parent>-broker-config; body = {body}"
    );

    // 2. Broker container args must reference --config-file.
    let containers = body["spec"]["template"]["spec"]["containers"]
        .as_array()
        .unwrap_or_else(|| panic!("containers present; body = {body}"));
    let broker = containers
        .iter()
        .find(|c| c["name"] == "broker")
        .unwrap_or_else(|| panic!("broker container missing; body = {body}"));
    let args = broker["args"]
        .as_array()
        .unwrap_or_else(|| panic!("broker args present; body = {body}"));
    let script = args
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        script.contains("--config-file=/run/crabka/broker.toml"),
        "--config-file flag missing from broker args; args = {script}"
    );
    assert!(
        !script.contains("--listen-addr"),
        "--listen-addr must not be present in broker args; args = {script}"
    );

    // 3. Broker container must mount broker-config at /etc/crabka/config.
    let volume_mounts = broker["volumeMounts"]
        .as_array()
        .unwrap_or_else(|| panic!("broker volumeMounts present; body = {body}"));
    let config_mount = volume_mounts
        .iter()
        .find(|m| m["name"] == "broker-config")
        .unwrap_or_else(|| panic!("broker-config volumeMount missing; mounts = {volume_mounts:?}"));
    assert!(
        config_mount["mountPath"] == "/etc/crabka/config",
        "broker-config must mount at /etc/crabka/config; body = {body}"
    );
    assert!(
        config_mount["readOnly"] == serde_json::Value::Bool(true),
        "broker-config mount must be readOnly; body = {body}"
    );

    // 4. CRABKA_ADVERTISED_LISTENER must not be in the broker container env.
    let env = broker["env"]
        .as_array()
        .unwrap_or_else(|| panic!("broker env present; body = {body}"));
    let has_advertised_listener = env
        .iter()
        .any(|e| e["name"] == "CRABKA_ADVERTISED_LISTENER");
    assert!(
        !has_advertised_listener,
        "CRABKA_ADVERTISED_LISTENER must not be in broker env (replaced by per-broker TOML); body = {body}"
    );

    assert!(state.remaining_rules() == 0);
}

/// A fresh cluster whose parent Kafka has an invalid `kafkaVersion` must
/// NOT bring up broker pods. The pool reconciler reads the parent's
/// `KafkaVersionValid=False` verdict and short-circuits to a `Ready=False`
/// status patch — no `StatefulSet` GET/PATCH — so the error surfaces as a CR
/// condition rather than a crash-looping (or silently-clamped) cluster.
#[tokio::test]
async fn pool_blocks_pod_creation_when_parent_version_invalid() {
    let parent = "demo";
    let pool_name = "brokers";
    let ns = "y";

    let rules = vec![
        // 1. GET parent Kafka -> KafkaVersionValid=False.
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body_version_invalid(parent, ns)),
        },
        // 2. The version gate blocks before any StatefulSet I/O; the only
        //    follow-up request is the pool status patch.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}/status"),
            response: json_response(200, &fake_pool_body(pool_name, ns, parent)),
        },
    ];

    let (ctx, state) = build_ctx(ns, rules);
    let pool = pool_cr(pool_name, ns, Some(parent), 1);

    reconcile(Arc::new(pool), ctx).await.unwrap();

    let observed = state.take_observed();
    // No StatefulSet was touched at all — no pods get formatted/created.
    for req in &observed {
        let uri = req.uri().to_string();
        assert!(
            !uri.contains("/statefulsets/"),
            "invalid-version path must not touch statefulsets: {uri}",
        );
    }

    // The pool surfaces Ready=False / KafkaVersionInvalid, echoing the
    // parent's verdict.
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkanodepools/{pool_name}/status"))
        })
        .expect("pool status PATCH must have been captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let cond = &body["status"]["conditions"][0];
    for (field, want) in [
        ("type", "Ready"),
        ("status", "False"),
        ("reason", "KafkaVersionInvalid"),
    ] {
        assert!(cond[field] == want, "field {field}; body = {body}");
    }

    assert!(state.remaining_rules() == 0);
}
