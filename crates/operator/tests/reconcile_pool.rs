//! Mocked-client integration tests for the `KafkaNodePool`
//! reconciler.
//!
//! Happy-path request sequence on a fresh pool:
//!   1. GET   kafkas/<parent>                  (-> 200 parent Kafka)
//!   2. GET   statefulsets/<parent>-<pool>     (pre-apply; monotonic-storage check)
//!   3. PATCH statefulsets/<parent>-<pool>     (SSA)
//!   4. GET   statefulsets/<parent>-<pool>     (post-apply status read)
//!   5. PATCH kafkanodepools/<pool>/status     (merge)
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
    MockRule, MockState, fake_parent_kafka_body, fake_pool_body, fake_sts_body,
    fake_sts_body_with_storage, fixture_ctx, json_response, mock_client, not_found_body,
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
            template: None,
            storage: None,
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

    vec![
        // 1. GET parent Kafka.
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{parent}"),
            response: json_response(200, &fake_parent_kafka_body(parent, namespace)),
        },
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

    assert!(state.remaining_rules() == 0);
}

#[tokio::test]
async fn pool_status_ready_when_sts_ready() {
    let state = MockState::new(happy_path_rules("demo", "brokers", "y", Some(1)));
    let mut ctx = fixture_ctx(mock_client(&state, "y"), "y");
    Arc::get_mut(&mut ctx.config)
        .expect("fixture owns operator config")
        .controller_dependency_requeue = crabka_units::millis(1_234);
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
        ("reason", "UnsupportedReplicaCount"),
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

    let rules = vec![
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

    let rules = vec![
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

    let rules = vec![
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

    let rules = vec![
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
