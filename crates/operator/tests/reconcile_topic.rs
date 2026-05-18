//! Slice 35: reconcile-level tests for the `KafkaTopic` controller.
//!
//! These tests assert the kube-side request sequence (status patches,
//! finalizer patches). Admin-client behavior is covered by the
//! integration test in `crates/client-admin/tests/round_trip.rs`.

use std::sync::Arc;

use crabka_operator::controller::topic::reconcile;
use crabka_operator::crd::{KafkaTopic, KafkaTopicSpec};
use http::{Method, Response};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState, fake_topic_body, fixture_ctx, json_response, mock_client, not_found_body,
};

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

/// Slice 35: a `KafkaTopic` with no `crabka.io/cluster` label must surface
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
    assert_eq!(cond["type"], "Ready");
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "MissingClusterLabel");
}

/// Slice 35: `KafkaTopic` referencing a Kafka that doesn't exist → status
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
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "ClusterNotReady");
}

/// Slice 35: `KafkaTopic` whose effective name is invalid
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
        assert!(
            !r.uri().to_string().contains("/kafkas/"),
            "InvalidTopicName must short-circuit before Kafka GET",
        );
    }
    let status_patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/kafkatopics/foo/status"))
        .expect("status PATCH");
    let body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let cond = &body["status"]["conditions"][0];
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "InvalidTopicName");
}
