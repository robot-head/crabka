//! Reconcile-level tests for the `KafkaTopic` controller.
//!
//! These tests assert the kube-side request sequence (status patches,
//! finalizer patches). Admin-client behavior is covered by the
//! integration test in `crates/client-admin/tests/round_trip.rs`.

use std::{sync::Arc, time::Duration};

use assert2::check;
use crabka_operator::{
    controller::topic::reconcile,
    crd::{KafkaTopic, KafkaTopicSpec},
};
use http::{Method, Response};
use kube::runtime::controller::Action;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState,
    fake_admin::{FakeAdminClient, RecordedCall, TopicState},
    fake_topic_body, fixture_ctx, json_response, mock_client, not_found_body,
};

/// JSON body shaped like a Ready Kafka with a single PLAIN internal
/// listener. Used by the finalizer-add-path test below; the topic
/// reconciler reads the Ready condition + listener bootstrap off this.
fn ready_kafka_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
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

fn topic(name: &str, ns: &str, cluster: Option<&str>) -> KafkaTopic {
    let mut kt = KafkaTopic::new(
        name,
        KafkaTopicSpec {
            topic_name: None,
            partitions: 3,
            replicas: 1,
            config: None,
            preserve_topic: false,
        },
    );
    kt.metadata.namespace = Some(ns.into());
    kt.metadata.uid = Some("topic-uid".into());
    if let Some(c) = cluster {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("crabka.io/cluster".into(), c.into());
        kt.metadata.labels = Some(labels);
    }
    kt
}

/// A `KafkaTopic` with no `crabka.io/cluster` label must surface
/// `MissingClusterLabel` and issue zero admin RPCs.
#[tokio::test]
async fn missing_cluster_label_sets_status() {
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/kafkatopics/foo/status".into(),
        response: json_response(200, &fake_topic_body("foo", "y")),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, "y");
    let ctx = Arc::new(fixture_ctx(client, "y"));

    let kt = topic("foo", "y", None);
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/kafkatopics/foo/status"))
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let cond = &body["status"]["conditions"][0];
    for (key, want) in [
        ("type", "Ready"),
        ("status", "False"),
        ("reason", "MissingClusterLabel"),
    ] {
        assert2::assert!(cond[key] == want);
    }
}

/// `KafkaTopic` referencing a Kafka that doesn't exist → status
/// `ClusterNotReady`; no admin RPCs.
#[tokio::test]
async fn cluster_not_found_sets_status_cluster_not_ready() {
    let rules = vec![
        // First, the reconcile validates the topic name (no API call there).
        // Then it GETs the Kafka -> 404.
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/missing-cluster".into(),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("kafka not found"))
                .expect("404 builds"),
        },
        // Status patch.
        MockRule {
            method: Method::PATCH,
            path_substr: "/kafkatopics/foo/status".into(),
            response: json_response(200, &fake_topic_body("foo", "y")),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, "y");
    let ctx = Arc::new(fixture_ctx(client, "y"));

    let kt = topic("foo", "y", Some("missing-cluster"));
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .rev()
        .find(|r| r.uri().to_string().contains("/kafkatopics/foo/status"))
        .expect("status PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let cond = &body["status"]["conditions"][0];
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("ClusterNotReady"));
}

/// `KafkaTopic` whose effective name is invalid
/// (`spec.topicName="."`) → status `InvalidTopicName`; no Kafka GET, no
/// admin RPCs.
#[tokio::test]
async fn invalid_topic_name_sets_status() {
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/kafkatopics/foo/status".into(),
        response: json_response(200, &fake_topic_body("foo", "y")),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, "y");
    let ctx = Arc::new(fixture_ctx(client, "y"));

    let mut kt = topic("foo", "y", Some("demo"));
    kt.spec.topic_name = Some(".".into());
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let observed = state.take_observed();
    for r in &observed {
        assert2::assert!(!r.uri().to_string().contains("/kafkas/"));
    }
    let status_patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/kafkatopics/foo/status"))
        .expect("status PATCH");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let cond = &body["status"]["conditions"][0];
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("InvalidTopicName"));
}

/// A `KafkaTopic` referencing a Ready Kafka but with no
/// finalizer set must PATCH `/kafkatopics/<name>` adding the finalizer
/// and request an immediate re-enter (`Action::requeue(Duration::ZERO)`).
/// No admin RPCs are issued — the finalizer-add path returns before any
/// connection.
#[tokio::test]
async fn finalizer_add_path_patches_metadata_and_requeues_immediately() {
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/demo".into(),
            response: json_response(200, &ready_kafka_body("demo", "y")),
        },
        MockRule {
            method: Method::PATCH,
            // Resource PATCH (not /status).
            path_substr: "/kafkatopics/foo".into(),
            response: json_response(200, &fake_topic_body("foo", "y")),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, "y");
    let ctx = Arc::new(fixture_ctx(client, "y"));

    let kt = topic("foo", "y", Some("demo"));
    let action = reconcile(Arc::new(kt), ctx).await.unwrap();
    assert2::assert!(action == Action::requeue(Duration::ZERO));

    let observed = state.take_observed();
    let finalizer_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/kafkatopics/foo")
                && !r.uri().to_string().contains("/status")
        })
        .expect("finalizer PATCH must have been captured");
    let body: serde_json::Value = serde_json::from_slice(finalizer_patch.body()).unwrap();
    let finalizers = &body["metadata"]["finalizers"];
    assert2::assert!(finalizers == &serde_json::json!(["crabka.io/topic-finalizer"]));

    // No /status PATCH — the finalizer-add path bails before any status
    // patch happens.
    assert2::assert!(
        !observed
            .iter()
            .any(|r| r.uri().to_string().contains("/kafkatopics/foo/status"))
    );
}

// ---- AdminClientLike-driven branch tests ----------------------------------
//
// These exercise the reconcile branches that need a live admin client:
// happy-path create, no-op, partition increase, immutable-field rejection,
// config diff, and delete-with/without-preserve. The fake `AdminClientLike`
// is pre-inserted into `ctx.admin_clients["demo"]`, so the real connect path
// is skipped — the reconcile just locks the cached handle and dispatches
// dynamically.

const CLUSTER: &str = "demo";
const NS: &str = "y";
const TOPIC_NAME: &str = "foo";

/// Build a `KafkaTopic` with the cluster label, finalizer, and the requested
/// partition/replicas/config values. Use this for branch tests; the
/// `topic()` helper above intentionally omits the finalizer so the
/// finalizer-add-path test works.
#[allow(clippy::needless_pass_by_value)]
fn topic_with_finalizer(
    name: &str,
    partitions: i32,
    replicas: i32,
    config: Option<std::collections::BTreeMap<String, String>>,
) -> KafkaTopic {
    let mut kt = KafkaTopic::new(
        name,
        KafkaTopicSpec {
            topic_name: None,
            partitions,
            replicas,
            config,
            preserve_topic: false,
        },
    );
    kt.metadata.namespace = Some(NS.into());
    kt.metadata.uid = Some("topic-uid".into());
    kt.metadata.generation = Some(1);
    kt.metadata.finalizers = Some(vec!["crabka.io/topic-finalizer".into()]);
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("crabka.io/cluster".into(), CLUSTER.into());
    kt.metadata.labels = Some(labels);
    kt
}

/// Wire a kube mock with the `GET kafkas/<cluster>` rule (returns a Ready
/// cluster) and one `PATCH /kafkatopics/<name>/status` rule. The reconcile
/// for the branch tests below issues exactly these two kube requests on
/// the happy/no-op/partition/immutable/config paths.
fn standard_kube_rules(topic_name: &str) -> Vec<MockRule> {
    vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkatopics/{topic_name}/status"),
            response: json_response(200, &fake_topic_body(topic_name, NS)),
        },
    ]
}

/// Wire a kube mock for the delete path: `GET kafkas/<cluster>` plus
/// `PATCH /kafkatopics/<name>` (finalizer removal — metadata patch, not
/// /status).
fn delete_kube_rules(topic_name: &str) -> Vec<MockRule> {
    vec![
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{CLUSTER}"),
            response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkatopics/{topic_name}"),
            response: json_response(200, &fake_topic_body(topic_name, NS)),
        },
    ]
}

/// Extract the body of the last `PATCH /kafkatopics/<name>/status` request
/// observed by the kube mock.
fn last_status_patch_body(state: &Arc<MockState>, topic_name: &str) -> serde_json::Value {
    let observed = state.take_observed();
    let patch = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkatopics/{topic_name}/status"))
        })
        .expect("status PATCH must have been captured");
    serde_json::from_slice(patch.body()).expect("status body parses as JSON")
}

/// Kafka Ready, topic absent → one `CreateTopics` call,
/// status `Ready=True topic_id=Some(...)`.
#[tokio::test]
async fn creates_topic_on_first_reconcile() {
    #[derive(Debug, PartialEq, Eq)]
    enum CallShape<'a> {
        Metadata(Vec<&'a str>),
        CreateTopics(Vec<(&'a str, i32, i32)>),
        Other,
    }

    let state = MockState::new(standard_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let kt = topic_with_finalizer(TOPIC_NAME, 3, 1, None);
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let actual = calls
        .iter()
        .map(|call| match call {
            RecordedCall::Metadata(topics) => {
                CallShape::Metadata(topics.iter().map(String::as_str).collect())
            }
            RecordedCall::CreateTopics(specs) => CallShape::CreateTopics(
                specs
                    .iter()
                    .map(|spec| (spec.name.as_str(), spec.partitions, spec.replicas))
                    .collect(),
            ),
            _ => CallShape::Other,
        })
        .collect::<Vec<_>>();
    assert2::assert!(
        actual
            == vec![
                CallShape::Metadata(vec![TOPIC_NAME]),
                CallShape::CreateTopics(vec![(TOPIC_NAME, 3, 1)]),
            ]
    );

    let body = last_status_patch_body(&state, TOPIC_NAME);
    let cond = &body["status"]["conditions"][0];
    assert2::assert!(cond["status"].as_str() == Some("True"));
    assert2::assert!(cond["reason"].as_str() == Some("Ready"));
    check!(
        body["status"]["topicId"].is_string(),
        "topicId should be a uuid string, got {:?}",
        body["status"]["topicId"],
    );
}

/// Kafka Ready, topic already matches spec exactly → no mutating
/// admin calls, status `Ready=True`.
#[tokio::test]
async fn noop_when_spec_matches_cluster() {
    let state = MockState::new(standard_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            config_overrides: std::collections::BTreeMap::new(),
        },
    );
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let kt = topic_with_finalizer(TOPIC_NAME, 3, 1, None);
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    for c in &calls {
        assert2::assert!(!matches!(
            c,
            RecordedCall::CreateTopics(_)
                | RecordedCall::DeleteTopics(_)
                | RecordedCall::CreatePartitions(_)
                | RecordedCall::IncrementalAlterConfigs(_)
        ));
    }
    // Metadata + DescribeConfigs (read-only) are expected.
    assert2::assert!(calls.iter().any(|c| matches!(c, RecordedCall::Metadata(_))));

    let body = last_status_patch_body(&state, TOPIC_NAME);
    let cond = &body["status"]["conditions"][0];
    assert2::assert!(cond["status"].as_str() == Some("True"));
    assert2::assert!(cond["reason"].as_str() == Some("Ready"));
}

/// current=3 partitions, spec=5 → one CreatePartitions(5) call,
/// status Ready.
#[tokio::test]
async fn partition_increase_triggers_create_partitions() {
    let state = MockState::new(standard_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            config_overrides: std::collections::BTreeMap::new(),
        },
    );
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let kt = topic_with_finalizer(TOPIC_NAME, 5, 1, None);
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let cp = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::CreatePartitions(ops) => Some(ops),
            _ => None,
        })
        .expect("CreatePartitions call expected");
    assert2::assert!(
        cp.iter()
            .map(|op| (op.name.as_str(), op.new_total_count))
            .collect::<Vec<_>>()
            == [(TOPIC_NAME, 5)]
    );

    let body = last_status_patch_body(&state, TOPIC_NAME);
    assert2::assert!(body["status"]["conditions"][0]["status"].as_str() == Some("True"));
    assert2::assert!(body["status"]["conditions"][0]["reason"].as_str() == Some("Ready"));
}

/// current=5 partitions, spec=2 → no mutating admin calls,
/// status `Ready=False reason=ImmutableFieldChanged`.
#[tokio::test]
async fn immutable_topic_field_change_cases() {
    for (_name, current_partitions, desired_partitions, desired_replicas) in [
        ("partition decrease", 5, 2, 1),
        ("replication-factor change", 3, 3, 2),
    ] {
        let state = MockState::new(standard_kube_rules(TOPIC_NAME));
        let client = mock_client(&state, NS);
        let ctx = Arc::new(fixture_ctx(client, NS));
        let fake = FakeAdminClient::new();
        fake.add_topic(
            TOPIC_NAME,
            TopicState {
                partitions: current_partitions,
                replicas: 1,
                topic_id: Some(uuid::Uuid::nil()),
                config_overrides: std::collections::BTreeMap::new(),
            },
        );
        let fake = Arc::new(tokio::sync::Mutex::new(fake));
        let fake_for_assert = fake.clone();
        ctx.insert_admin_client_for_test(CLUSTER, fake).await;

        let topic = topic_with_finalizer(TOPIC_NAME, desired_partitions, desired_replicas, None);
        reconcile(Arc::new(topic), ctx).await.unwrap();

        let calls = fake_for_assert.lock().await.calls();
        let has_mutation = calls.iter().any(|call| {
            matches!(
                call,
                RecordedCall::CreateTopics(_)
                    | RecordedCall::DeleteTopics(_)
                    | RecordedCall::CreatePartitions(_)
                    | RecordedCall::IncrementalAlterConfigs(_)
            )
        });
        let body = last_status_patch_body(&state, TOPIC_NAME);
        let condition = &body["status"]["conditions"][0];
        assert2::assert!(!has_mutation);
        assert2::assert!(condition["status"].as_str() == Some("False"));
        assert2::assert!(condition["reason"].as_str() == Some("ImmutableFieldChanged"));
    }
}

/// current overrides `{foo: 1}`, desired `{bar: 2}` →
/// `IncrementalAlterConfigs` with Set(bar=2) and Delete(foo).
#[tokio::test]
async fn config_diff_sets_and_deletes() {
    let state = MockState::new(standard_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            config_overrides: std::collections::BTreeMap::from([(
                "foo".to_string(),
                "1".to_string(),
            )]),
        },
    );
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let desired = std::collections::BTreeMap::from([("bar".to_string(), "2".to_string())]);
    let kt = topic_with_finalizer(TOPIC_NAME, 3, 1, Some(desired));
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let ops = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::IncrementalAlterConfigs(ops) => Some(ops.clone()),
            _ => None,
        })
        .expect("IncrementalAlterConfigs call expected");

    let has_set_bar = ops.iter().any(|op| {
        matches!(
            op,
            crabka_client_admin::IncrementalAlterOp::Set { topic, key, value }
                if topic == TOPIC_NAME && key == "bar" && value == "2"
        )
    });
    let has_delete_foo = ops.iter().any(|op| {
        matches!(
            op,
            crabka_client_admin::IncrementalAlterOp::Delete { topic, key }
                if topic == TOPIC_NAME && key == "foo"
        )
    });
    assert2::assert!(has_set_bar);
    assert2::assert!(has_delete_foo);

    let body = last_status_patch_body(&state, TOPIC_NAME);
    assert2::assert!(body["status"]["conditions"][0]["status"].as_str() == Some("True"));
    assert2::assert!(body["status"]["conditions"][0]["reason"].as_str() == Some("Ready"));
}

/// deletionTimestamp set, preserveTopic=false → one `DeleteTopics`
/// call + finalizer removed via metadata PATCH.
#[tokio::test]
async fn delete_with_finalizer_calls_delete_topics() {
    let state = MockState::new(delete_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            config_overrides: std::collections::BTreeMap::new(),
        },
    );
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let mut kt = topic_with_finalizer(TOPIC_NAME, 3, 1, None);
    kt.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        "2026-05-18T00:00:00Z".parse().unwrap(),
    ));
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    let dt = calls
        .iter()
        .find_map(|c| match c {
            RecordedCall::DeleteTopics(names) => Some(names.clone()),
            _ => None,
        })
        .expect("DeleteTopics call expected");
    assert2::assert!(dt == vec![TOPIC_NAME.to_string()]);

    // Finalizer-removal patch: metadata.finalizers=[].
    let observed = state.take_observed();
    let metadata_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkatopics/{TOPIC_NAME}"))
                && !r.uri().to_string().contains("/status")
        })
        .expect("metadata PATCH for finalizer removal");
    let body: serde_json::Value = serde_json::from_slice(metadata_patch.body()).unwrap();
    assert2::assert!(body["metadata"]["finalizers"] == serde_json::json!([]));
}

// ---- Broker-error / transport-error reconcile tests ----------------------
//
// These cover the per-RPC failure branches inside the reconcile (T3-fix
// eviction, BrokerError status mapping, finalizer-cleanup robustness when
// DeleteTopics fails). Each test injects an error on a specific RPC via
// the `FakeAdminClient` and asserts the status / requeue / eviction the
// reconcile takes in response.

/// Kafka Ready, topic absent, broker rejects `CreateTopics` with
/// `TOPIC_ALREADY_EXISTS` → status `Ready=False reason=BrokerError`,
/// message references the API + error name.
#[tokio::test]
async fn creates_topic_broker_error_surfaces_in_status() {
    let state = MockState::new(standard_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.inject_create_topics_broker_error(36, "TOPIC_ALREADY_EXISTS", None);
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let kt = topic_with_finalizer(TOPIC_NAME, 3, 1, None);
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let body = last_status_patch_body(&state, TOPIC_NAME);
    let cond = &body["status"]["conditions"][0];
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("BrokerError"));
    let msg = cond["message"].as_str().unwrap();
    assert2::assert!(msg.contains("CreateTopics"));
    assert2::assert!(msg.contains("TOPIC_ALREADY_EXISTS"));
}

/// topic exists at 3 partitions, spec=5; broker rejects
/// `CreatePartitions` with `INVALID_PARTITIONS` → status
/// `Ready=False reason=BrokerError` referencing `CreatePartitions`.
#[tokio::test]
async fn create_partitions_broker_error_surfaces_in_status() {
    let state = MockState::new(standard_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            config_overrides: std::collections::BTreeMap::new(),
        },
    );
    fake.inject_create_partitions_broker_error(37, "INVALID_PARTITIONS", None);
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let kt = topic_with_finalizer(TOPIC_NAME, 5, 1, None);
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let body = last_status_patch_body(&state, TOPIC_NAME);
    let cond = &body["status"]["conditions"][0];
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("BrokerError"));
    let msg = cond["message"].as_str().unwrap();
    assert2::assert!(msg.contains("CreatePartitions"));
    assert2::assert!(msg.contains("INVALID_PARTITIONS"));
}

/// topic matches spec but has a stale config override; broker
/// rejects `IncrementalAlterConfigs` with `INVALID_CONFIG` → status
/// `Ready=False reason=BrokerError`.
#[tokio::test]
async fn incremental_alter_configs_broker_error_surfaces_in_status() {
    let state = MockState::new(standard_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            // current has "foo=1", desired below has "bar=2" → both a SET
            // and a DELETE op are emitted, exercising the diff path.
            config_overrides: std::collections::BTreeMap::from([(
                "foo".to_string(),
                "1".to_string(),
            )]),
        },
    );
    fake.inject_incremental_alter_configs_broker_error(40, "INVALID_CONFIG", None);
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let desired = std::collections::BTreeMap::from([("bar".to_string(), "2".to_string())]);
    let kt = topic_with_finalizer(TOPIC_NAME, 3, 1, Some(desired));
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let body = last_status_patch_body(&state, TOPIC_NAME);
    let cond = &body["status"]["conditions"][0];
    assert2::assert!(cond["status"].as_str() == Some("False"));
    assert2::assert!(cond["reason"].as_str() == Some("BrokerError"));
    let msg = cond["message"].as_str().unwrap();
    assert2::assert!(msg.contains("IncrementalAlterConfigs"));
    assert2::assert!(msg.contains("INVALID_CONFIG"));
}

/// `describe_configs` returns `AdminError::Broker` → the
/// reconcile logs + requeues 15s WITHOUT updating status.
#[tokio::test]
async fn describe_configs_broker_error_requeues_without_status_update() {
    // Only the cluster GET is wired: a status PATCH here would surface as
    // an unexpected request (404) and the reconcile would return an error,
    // so the absence of any status patch is itself the assertion.
    let rules = vec![MockRule {
        method: Method::GET,
        path_substr: format!("/kafkas/{CLUSTER}"),
        response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            config_overrides: std::collections::BTreeMap::new(),
        },
    );
    fake.inject_describe_configs_broker_error(7, "REQUEST_TIMED_OUT", None);
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    ctx.insert_admin_client_for_test(CLUSTER, fake.clone())
        .await;

    let kt = topic_with_finalizer(TOPIC_NAME, 3, 1, None);
    let action = reconcile(Arc::new(kt), ctx.clone()).await.unwrap();
    assert2::assert!(action == Action::requeue(Duration::from_secs(15)));

    // No /status PATCH observed.
    let observed = state.take_observed();
    assert2::assert!(!observed.iter().any(|r| {
        r.uri()
            .to_string()
            .contains(&format!("/kafkatopics/{TOPIC_NAME}/status"))
    }));

    // Broker (non-Transport) errors do NOT evict the cached admin client.
    assert2::assert!(ctx.admin_clients.lock().await.contains_key(CLUSTER));
}

/// `DeleteTopics` fails during finalizer cleanup with a broker
/// error → the finalizer is STILL removed (best-effort path); the
/// `DeleteTopics` call is still observed in the fake's call log.
#[tokio::test]
async fn delete_topics_broker_error_during_finalizer_does_not_block_cleanup() {
    let state = MockState::new(delete_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            config_overrides: std::collections::BTreeMap::new(),
        },
    );
    // Inject a generic broker failure on delete_topics; current code only
    // logs and proceeds with finalizer removal regardless of outcome.
    fake.inject_delete_topics_broker_error(3, "UNKNOWN_TOPIC_OR_PARTITION", None);
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let mut kt = topic_with_finalizer(TOPIC_NAME, 3, 1, None);
    kt.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        "2026-05-18T00:00:00Z".parse().unwrap(),
    ));
    reconcile(Arc::new(kt), ctx).await.unwrap();

    // DeleteTopics WAS called (even though it failed).
    let calls = fake_for_assert.lock().await.calls();
    assert2::assert!(calls.iter().any(
        |c| matches!(c, RecordedCall::DeleteTopics(names) if names == &vec![TOPIC_NAME.to_string()])
    ));

    // Finalizer-removal patch still issued.
    let observed = state.take_observed();
    let metadata_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkatopics/{TOPIC_NAME}"))
                && !r.uri().to_string().contains("/status")
        })
        .expect("metadata PATCH for finalizer removal");
    let body: serde_json::Value = serde_json::from_slice(metadata_patch.body()).unwrap();
    assert2::assert!(body["metadata"]["finalizers"] == serde_json::json!([]));
}

/// A Transport error on `metadata` → reconcile
/// requeues 15s, issues NO status patch, and EVICTS the cached admin
/// client (so the next reconcile reopens the connection).
#[tokio::test]
async fn metadata_transport_error_requeues_and_evicts_admin_client() {
    let rules = vec![MockRule {
        method: Method::GET,
        path_substr: format!("/kafkas/{CLUSTER}"),
        response: json_response(200, &ready_kafka_body(CLUSTER, NS)),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.inject_metadata_transport_error();
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;
    // Sanity: cache primed before reconcile.
    assert2::assert!(ctx.admin_clients.lock().await.contains_key(CLUSTER));

    let kt = topic_with_finalizer(TOPIC_NAME, 3, 1, None);
    let action = reconcile(Arc::new(kt), ctx.clone()).await.unwrap();
    assert2::assert!(action == Action::requeue(Duration::from_secs(15)));

    let observed = state.take_observed();
    assert2::assert!(!observed.iter().any(|r| {
        r.uri()
            .to_string()
            .contains(&format!("/kafkatopics/{TOPIC_NAME}/status"))
    }));

    // T3-fix: Transport errors evict the cached admin client.
    assert2::assert!(!ctx.admin_clients.lock().await.contains_key(CLUSTER));
}

/// deletionTimestamp set, preserveTopic=true → no `DeleteTopics`
/// call, finalizer still removed.
#[tokio::test]
async fn delete_with_preserve_topic_skips_delete_topics() {
    let state = MockState::new(delete_kube_rules(TOPIC_NAME));
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let fake = FakeAdminClient::new();
    fake.add_topic(
        TOPIC_NAME,
        TopicState {
            partitions: 3,
            replicas: 1,
            topic_id: Some(uuid::Uuid::nil()),
            config_overrides: std::collections::BTreeMap::new(),
        },
    );
    let fake = Arc::new(tokio::sync::Mutex::new(fake));
    let fake_for_assert = fake.clone();
    ctx.insert_admin_client_for_test(CLUSTER, fake).await;

    let mut kt = topic_with_finalizer(TOPIC_NAME, 3, 1, None);
    kt.spec.preserve_topic = true;
    kt.metadata.deletion_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        "2026-05-18T00:00:00Z".parse().unwrap(),
    ));
    reconcile(Arc::new(kt), ctx).await.unwrap();

    let calls = fake_for_assert.lock().await.calls();
    assert2::assert!(
        !calls
            .iter()
            .any(|c| matches!(c, RecordedCall::DeleteTopics(_)))
    );

    let observed = state.take_observed();
    let metadata_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkatopics/{TOPIC_NAME}"))
                && !r.uri().to_string().contains("/status")
        })
        .expect("metadata PATCH for finalizer removal");
    let body: serde_json::Value = serde_json::from_slice(metadata_patch.body()).unwrap();
    assert2::assert!(body["metadata"]["finalizers"] == serde_json::json!([]));
}
