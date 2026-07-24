//! Reconcile-level tests for the `SchemaRegistry` controller.
//!
//! These assert the kube-side request sequence the reconciler issues
//! (Kafka GET, Service/Deployment SSA applies, status patch) and the
//! rendered Deployment container args / env / Secret mounts.

use std::sync::Arc;

use assert2::assert;
use crabka_operator::{
    controller::{common::ReconcileError, schema_registry::reconcile},
    crd::{
        BearerAuthn, BearerMode, SchemaRegistry, SchemaRegistryAuthn, SchemaRegistryAuthz,
        SchemaRegistryHealthChecks, SchemaRegistryRuntime, SchemaRegistrySpec,
    },
};
use http::Method;

#[path = "shared/mod.rs"]
mod shared;
use shared::{MockRule, MockState, fixture_ctx, json_response, mock_client};

const NS: &str = "default";
const CLUSTER: &str = "demo";

fn sr(name: &str, cluster: Option<&str>) -> SchemaRegistry {
    let mut cr = SchemaRegistry::new(
        name,
        SchemaRegistrySpec {
            replicas: 1,
            image: None,
            bootstrap_servers: None,
            schemas_topic: None,
            schemas_topic_replication_factor: Some(1),
            group_id: None,
            runtime: None,
            client_id: None,
            health_checks: None,
            kafka_client: None,
            tls: None,
            authentication: None,
            authorization: None,
            resources: None,
        },
    );
    cr.metadata.namespace = Some(NS.into());
    cr.metadata.uid = Some("uid-1".into());
    cr.metadata.generation = Some(1);
    if let Some(c) = cluster {
        cr.metadata.labels = Some([("crabka.io/cluster".to_string(), c.to_string())].into());
    }
    cr
}

fn schema_registry_apply_rules() -> Vec<MockRule> {
    vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},
                "status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},
                "status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ]
}

fn valid_runtime() -> SchemaRegistryRuntime {
    SchemaRegistryRuntime {
        election_session_timeout_ms: Some(12_000),
        election_rebalance_timeout_ms: Some(40_000),
        election_heartbeat_interval_ms: Some(2_000),
        election_reconnect_backoff_ms: Some(750),
        store_reader_retry_backoff_ms: Some(333),
        store_reader_fetch_max_wait_ms: Some(777),
        store_reader_fetch_max_bytes: Some(2_097_152),
        schemas_topic_create_timeout_ms: Some(22_000),
        forward_max_body_bytes: Some(3_145_728),
        default_compatibility_level: Some("FULL".into()),
        default_mode: Some("IMPORT".into()),
    }
}

#[tokio::test]
async fn runtime_policy_renders_exact_flags_and_probe_timings() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.runtime = Some(valid_runtime());
    cr.spec.client_id = Some("registry-production".into());
    cr.spec.health_checks = Some(SchemaRegistryHealthChecks {
        readiness_initial_delay_seconds: Some(3),
        readiness_period_seconds: Some(7),
        liveness_initial_delay_seconds: Some(9),
        liveness_period_seconds: Some(11),
    });
    let state = MockState::new(schema_registry_apply_rules());
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let deployment = observed
        .iter()
        .find(|request| request.uri().to_string().contains("/deployments/sr1-sr"))
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(deployment.body()).unwrap();
    let container = &body["spec"]["template"]["spec"]["containers"][0];
    assert!(
        container["args"]
            == serde_json::json!([
                "--bootstrap-servers=ext:9092",
                "--listen-addr=0.0.0.0:8081",
                "--schemas-topic-rf=1",
                "--election-session-timeout-ms=12000",
                "--election-rebalance-timeout-ms=40000",
                "--election-heartbeat-interval-ms=2000",
                "--election-reconnect-backoff-ms=750",
                "--store-reader-retry-backoff-ms=333",
                "--store-reader-fetch-max-wait-ms=777",
                "--store-reader-fetch-max-bytes=2097152",
                "--schemas-topic-create-timeout-ms=22000",
                "--forward-max-body-bytes=3145728",
                "--default-compatibility-level=FULL",
                "--default-mode=IMPORT",
                "--client-id=registry-production",
            ])
    );
    assert!(
        container["readinessProbe"]
            == serde_json::json!({
                "tcpSocket": { "port": 8081 },
                "initialDelaySeconds": 3,
                "periodSeconds": 7,
            })
    );
    assert!(
        container["livenessProbe"]
            == serde_json::json!({
                "tcpSocket": { "port": 8081 },
                "initialDelaySeconds": 9,
                "periodSeconds": 11,
            })
    );
}

fn runtime_with(field: &str, value: serde_json::Value) -> SchemaRegistryRuntime {
    let mut runtime = serde_json::to_value(valid_runtime()).unwrap();
    runtime[field] = value;
    serde_json::from_value(runtime).unwrap()
}

async fn assert_schema_registry_config_invalid(cr: SchemaRegistry) {
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/schemaregistries/sr1/status".into(),
        response: json_response(
            200,
            &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
        ),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    let error = reconcile(Arc::new(cr), ctx).await.unwrap_err();

    assert!(matches!(
        error,
        ReconcileError::SchemaRegistryConfigInvalid(_)
    ));
    let observed = state.take_observed();
    assert!(!observed.iter().any(|request| {
        let uri = request.uri().to_string();
        uri.contains("/deployments/") || uri.contains("/services/")
    }));
    let status = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/schemaregistries/sr1/status")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    let ready = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert!(ready["reason"] == "SchemaRegistryConfigInvalid");
}

#[tokio::test]
async fn runtime_invalid_policy_is_rejected_before_deployment() {
    for field in [
        "electionSessionTimeoutMs",
        "electionRebalanceTimeoutMs",
        "electionHeartbeatIntervalMs",
        "electionReconnectBackoffMs",
        "storeReaderRetryBackoffMs",
        "storeReaderFetchMaxWaitMs",
        "storeReaderFetchMaxBytes",
        "schemasTopicCreateTimeoutMs",
        "forwardMaxBodyBytes",
    ] {
        let mut cr = sr("sr1", Some(CLUSTER));
        cr.spec.bootstrap_servers = Some("ext:9092".into());
        cr.spec.runtime = Some(runtime_with(field, serde_json::json!(0)));
        assert_schema_registry_config_invalid(cr).await;
    }

    for runtime in [
        runtime_with("electionHeartbeatIntervalMs", serde_json::json!(12_000)),
        runtime_with("electionSessionTimeoutMs", serde_json::json!(40_001)),
        runtime_with("defaultCompatibilityLevel", serde_json::json!("INVALID")),
        runtime_with("defaultMode", serde_json::json!("INVALID")),
    ] {
        let mut cr = sr("sr1", Some(CLUSTER));
        cr.spec.bootstrap_servers = Some("ext:9092".into());
        cr.spec.runtime = Some(runtime);
        assert_schema_registry_config_invalid(cr).await;
    }

    let mut empty_client = sr("sr1", Some(CLUSTER));
    empty_client.spec.bootstrap_servers = Some("ext:9092".into());
    empty_client.spec.client_id = Some(String::new());
    assert_schema_registry_config_invalid(empty_client).await;

    for health_checks in [
        SchemaRegistryHealthChecks {
            readiness_initial_delay_seconds: Some(-1),
            readiness_period_seconds: None,
            liveness_initial_delay_seconds: None,
            liveness_period_seconds: None,
        },
        SchemaRegistryHealthChecks {
            readiness_initial_delay_seconds: None,
            readiness_period_seconds: Some(0),
            liveness_initial_delay_seconds: None,
            liveness_period_seconds: None,
        },
        SchemaRegistryHealthChecks {
            readiness_initial_delay_seconds: None,
            readiness_period_seconds: None,
            liveness_initial_delay_seconds: Some(-1),
            liveness_period_seconds: None,
        },
        SchemaRegistryHealthChecks {
            readiness_initial_delay_seconds: None,
            readiness_period_seconds: None,
            liveness_initial_delay_seconds: None,
            liveness_period_seconds: Some(0),
        },
    ] {
        let mut cr = sr("sr1", Some(CLUSTER));
        cr.spec.bootstrap_servers = Some("ext:9092".into());
        cr.spec.health_checks = Some(health_checks);
        assert_schema_registry_config_invalid(cr).await;
    }

    let mut invalid_rf = sr("sr1", Some(CLUSTER));
    invalid_rf.spec.bootstrap_servers = Some("ext:9092".into());
    invalid_rf.spec.schemas_topic_replication_factor = Some(0);
    assert_schema_registry_config_invalid(invalid_rf).await;

    let mut invalid_jwks = sr("sr1", Some(CLUSTER));
    invalid_jwks.spec.bootstrap_servers = Some("ext:9092".into());
    invalid_jwks.spec.authentication = Some(SchemaRegistryAuthn {
        require_auth: false,
        realm: None,
        basic: None,
        bearer: Some(BearerAuthn {
            mode: BearerMode::Jwks,
            principal_claim: None,
            jwks_endpoint_uri: None,
            jwks_valid_issuer: None,
            jwks_expected_audience: None,
            jwks_tls_secret_name: None,
            jwks_principal_claim: None,
            jwks_refresh_ms: Some(0),
        }),
    });
    assert_schema_registry_config_invalid(invalid_jwks).await;

    let mut invalid_acl = sr("sr1", Some(CLUSTER));
    invalid_acl.spec.bootstrap_servers = Some("ext:9092".into());
    invalid_acl.spec.authorization = Some(SchemaRegistryAuthz {
        enabled: true,
        super_users: Vec::new(),
        acl_refresh_seconds: Some(0),
    });
    assert_schema_registry_config_invalid(invalid_acl).await;
}

/// A Ready Kafka body whose internal listener exposes a bootstrap address.
fn ready_kafka_body(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1", "kind": "Kafka",
        "metadata": { "name": name, "namespace": NS },
        "spec": { "kafkaVersion": "3.7.0" },
        "status": {
            "conditions": [{ "type": "Ready", "status": "True", "reason": "Ready",
                "message": "ok", "lastTransitionTime": "2026-01-01T00:00:00Z" }],
            "listeners": [{ "name": "PLAIN", "type": "internal", "bootstrapServers": "demo-broker-headless.default.svc.cluster.local:9092" }]
        }
    })
}

#[tokio::test]
async fn kafka_present_but_not_ready_gates_with_no_children() {
    // Labeled CR + a Kafka that is NOT Ready -> internal_listener_bootstrap
    // returns None -> KafkaNotReady gate; NO child resources are applied
    // (no Service/Deployment mock rules, so any apply attempt would 404 and
    // fail the reconcile).
    let not_ready = serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1", "kind": "Kafka",
        "metadata": { "name": CLUSTER, "namespace": NS },
        "spec": { "kafkaVersion": "3.7.0" },
        "status": {
            "conditions": [{ "type": "Ready", "status": "False", "reason": "Progressing",
                "message": "starting", "lastTransitionTime": "2026-01-01T00:00:00Z" }],
            "listeners": []
        }
    });
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/demo".into(),
            response: json_response(200, &not_ready),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({
                    "apiVersion": "crabka.io/v1alpha1", "kind": "SchemaRegistry",
                    "metadata": { "name": "sr1", "namespace": NS }, "spec": { "replicas": 1 }
                }),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    reconcile(Arc::new(sr("sr1", Some(CLUSTER))), ctx)
        .await
        .unwrap();

    let observed = state.take_observed();
    // The gate must fire BEFORE any child is applied.
    assert!(
        !observed
            .iter()
            .any(|r| r.uri().to_string().contains("/services/"))
    );
    assert!(
        !observed
            .iter()
            .any(|r| r.uri().to_string().contains("/deployments/"))
    );
    let patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status"))
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(patch.body()).unwrap();
    let conds = body["status"]["conditions"].as_array().unwrap();
    let kr = conds.iter().find(|c| c["type"] == "KafkaReady").unwrap();
    assert!(kr["status"] == "False");
    assert!(kr["reason"] == "KafkaNotReady");
}

#[tokio::test]
async fn optional_topic_group_and_bearer_render_to_args() {
    // Exercise the optional schemas-topic / group-id arg branches and the
    // (unsecured) Bearer authn branch. bootstrap override skips the Kafka GET.
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.schemas_topic = Some("custom-schemas".into());
    cr.spec.group_id = Some("sr-grp".into());
    cr.spec.authentication = Some(crabka_operator::crd::SchemaRegistryAuthn {
        require_auth: false,
        realm: None,
        basic: None,
        bearer: Some(crabka_operator::crd::BearerAuthn {
            mode: crabka_operator::crd::BearerMode::Unsecured,
            principal_claim: Some("email".into()),
            jwks_endpoint_uri: None,
            jwks_valid_issuer: None,
            jwks_expected_audience: None,
            jwks_tls_secret_name: None,
            jwks_principal_claim: None,
            jwks_refresh_ms: None,
        }),
    });
    // kube-rs deserializes each SSA response into its typed object, which
    // requires `metadata` (ObjectMeta is non-optional) — include it.
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let args = body["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array()
        .unwrap();
    let joined = args
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    for needle in [
        "--schemas-topic=custom-schemas",
        "--group-id=sr-grp",
        "--bearer=unsecured",
        "--bearer-principal-claim=email",
    ] {
        assert!(
            joined.contains(needle),
            "needle {needle:?}, joined: {joined}"
        );
    }
}

#[tokio::test]
async fn missing_cluster_label_sets_status() {
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/schemaregistries/sr1/status".into(),
        response: json_response(
            200,
            &serde_json::json!({
                "apiVersion": "crabka.io/v1alpha1", "kind": "SchemaRegistry",
                "metadata": { "name": "sr1", "namespace": NS }, "spec": { "replicas": 1 }
            }),
        ),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    reconcile(Arc::new(sr("sr1", None)), ctx).await.unwrap();

    let observed = state.take_observed();
    let patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status"))
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(patch.body()).unwrap();
    let ready = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap();
    assert!(ready["status"] == "False");
    assert!(ready["reason"] == "MissingClusterLabel");
}

#[tokio::test]
async fn renders_children_when_kafka_ready() {
    // FIFO: GET Kafka (ready) → apply headless svc → apply clusterip svc →
    // apply deployment → GET deployment (status) → PATCH status.
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/demo".into(),
            response: json_response(200, &ready_kafka_body(CLUSTER)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},
                "status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},
                "status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));

    reconcile(Arc::new(sr("sr1", Some(CLUSTER))), ctx)
        .await
        .unwrap();

    let observed = state.take_observed();
    // The Deployment apply body carries the derived --bootstrap-servers arg.
    let dep = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let args = body["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array()
        .unwrap();
    let joined = args
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        joined.contains("--bootstrap-servers=demo-broker-headless.default.svc.cluster.local:9092")
    );
    assert!(joined.contains("--schemas-topic-rf=1"));
    // advertised-url env uses $(POD_NAME) interpolation.
    let env = body["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .unwrap();
    let adv = env
        .iter()
        .find(|e| e["name"] == "SCHEMA_REGISTRY_ADVERTISED_URL")
        .unwrap();
    assert!(
        adv["value"]
            .as_str()
            .unwrap()
            .contains("$(POD_NAME).sr1-sr-headless.default.svc.cluster.local:8081")
    );
    // Status rolled up to Ready/Available.
    let st = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status"))
        .unwrap();
    let sb: serde_json::Value = serde_json::from_slice(st.body()).unwrap();
    let ready = sb["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap();
    assert!(ready["status"] == "True");
}

#[tokio::test]
// the typed-security spec + mock-rule enumeration make the length inherent
async fn full_security_fields_render_to_args_and_mounts() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into()); // skip the Kafka GET
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: Some("sr-tls".into()),
        issuer_ref: None,
        client_auth: Some(crabka_operator::crd::TlsClientAuth::Required),
        client_ca_secret_name: Some("sr-client-ca".into()),
    });
    cr.spec.authentication = Some(crabka_operator::crd::SchemaRegistryAuthn {
        require_auth: true,
        realm: Some("R".into()),
        basic: Some(crabka_operator::crd::BasicAuthn {
            users_secret_name: "sr-users".into(),
            users_secret_key: None,
        }),
        bearer: None,
    });
    cr.spec.authorization = Some(crabka_operator::crd::SchemaRegistryAuthz {
        enabled: true,
        super_users: vec!["User:admin".into()],
        acl_refresh_seconds: Some(15),
    });
    // No Kafka GET rule needed (bootstrap override). Provide the apply/status rules.
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},
                "status":{"replicas":1,"readyReplicas":0}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},
                "status":{"replicas":1,"readyReplicas":0}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let c = &body["spec"]["template"]["spec"]["containers"][0];
    let joined = c["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    for needle in [
        "--tls-cert=/etc/sr/tls/tls.crt",
        "--tls-client-auth=required",
        "--tls-client-ca=/etc/sr/client-ca/ca.crt",
        "--require-auth",
        "--basic-auth-file=/etc/sr/basic/users",
        "--authz",
        "--super-user=User:admin",
        "--acl-refresh-secs=15",
    ] {
        assert!(
            joined.contains(needle),
            "needle {needle:?}, joined: {joined}"
        );
    }
    // Mounts present for tls/client-ca/basic.
    let mounts = c["volumeMounts"].as_array().unwrap();
    let mount_paths: Vec<&str> = mounts
        .iter()
        .map(|m| m["mountPath"].as_str().unwrap())
        .collect();
    for path in ["/etc/sr/tls", "/etc/sr/client-ca", "/etc/sr/basic"] {
        assert!(
            mount_paths.contains(&path),
            "mount {path:?}, mounts: {mount_paths:?}"
        );
    }
}

#[tokio::test]
async fn kafka_client_missing_when_absent() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let args = body["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array()
        .unwrap();
    let joined = args
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(!joined.contains("--kafka-security-protocol"));
    assert!(!joined.contains("--kafka-sasl-mechanism"));
}

#[tokio::test]
async fn kafka_client_sasl_ssl_renders_to_args_and_env() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.kafka_client = Some(crabka_operator::crd::SchemaRegistryKafkaClient {
        security_protocol: Some("SASL_SSL".into()),
        sasl: Some(crabka_operator::crd::KafkaClientSasl {
            mechanism: "PLAIN".into(),
            secret_ref: "kafka-creds".into(),
        }),
        tls: Some(crabka_operator::crd::KafkaClientTls {
            ca_secret_name: Some("kafka-ca".into()),
            server_name_override: Some("broker.internal".into()),
        }),
    });
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let c = &body["spec"]["template"]["spec"]["containers"][0];
    let joined = c["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    // Args
    for needle in [
        "--kafka-security-protocol=SASL_SSL",
        "--kafka-sasl-mechanism=PLAIN",
        "--kafka-tls-ca=/etc/sr/kafka-tls/ca.crt",
        "--kafka-tls-server-name=broker.internal",
    ] {
        assert!(
            joined.contains(needle),
            "needle {needle:?}, joined: {joined}"
        );
    }
    // Env: SASL creds via secretKeyRef
    let env = c["env"].as_array().unwrap();
    let sasl_user = env
        .iter()
        .find(|e| e["name"] == "SCHEMA_REGISTRY_KAFKA_SASL_USERNAME")
        .unwrap();
    assert_eq!(
        sasl_user["valueFrom"]["secretKeyRef"]["name"],
        "kafka-creds"
    );
    assert_eq!(sasl_user["valueFrom"]["secretKeyRef"]["key"], "username");
    let sasl_pass = env
        .iter()
        .find(|e| e["name"] == "SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD")
        .unwrap();
    assert_eq!(
        sasl_pass["valueFrom"]["secretKeyRef"]["name"],
        "kafka-creds"
    );
    assert_eq!(sasl_pass["valueFrom"]["secretKeyRef"]["key"], "password");
    // Volume + mount for kafka-tls CA
    let vols = body["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap();
    assert!(
        vols.iter().any(|v| v["name"] == "kafka-tls"),
        "expected kafka-tls volume"
    );
    let mounts = c["volumeMounts"].as_array().unwrap();
    assert!(
        mounts.iter().any(|m| m["mountPath"] == "/etc/sr/kafka-tls"),
        "expected kafka-tls mount"
    );
}

#[tokio::test]
async fn secret_name_and_issuer_ref_mutual_exclusion() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: Some("explicit-secret".into()),
        issuer_ref: Some(crabka_operator::crd::CertManagerIssuerRef {
            name: "my-issuer".into(),
            kind: None,
            group: None,
        }),
        client_auth: None,
        client_ca_secret_name: None,
    });
    let rules = vec![MockRule {
        method: Method::PATCH,
        path_substr: "/schemaregistries/sr1/status".into(),
        response: json_response(
            200,
            &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
        ),
    }];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    assert!(
        !observed
            .iter()
            .any(|r| r.uri().to_string().contains("/deployments/"))
    );
    let patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status"))
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(patch.body()).unwrap();
    let ready = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap();
    assert_eq!(ready["reason"], "InvalidSpec");
}

#[tokio::test]
async fn issuer_ref_creates_certificate_cr_and_waits() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: None,
        issuer_ref: Some(crabka_operator::crd::CertManagerIssuerRef {
            name: "my-issuer".into(),
            kind: Some("ClusterIssuer".into()),
            group: None,
        }),
        client_auth: None,
        client_ca_secret_name: None,
    });
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/certificates/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"cert-manager.io/v1","kind":"Certificate","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/secrets/sr1-sr-tls".into(),
            response: json_response(
                404,
                &serde_json::json!({"kind":"Status","status":"Failure","reason":"NotFound"}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    assert!(
        observed
            .iter()
            .any(|r| r.method() == Method::PATCH
                && r.uri().to_string().contains("/certificates/sr1-sr")),
        "expected Certificate CR PATCH"
    );
    assert!(
        !observed
            .iter()
            .any(|r| r.uri().to_string().contains("/deployments/")),
        "expected no deployment while WaitingForCert"
    );
    let patch = observed
        .iter()
        .find(|r| r.uri().to_string().contains("/schemaregistries/sr1/status"))
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(patch.body()).unwrap();
    let ready = body["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap();
    assert_eq!(ready["reason"], "WaitingForCert");
}

#[tokio::test]
async fn issuer_ref_with_cert_secret_ready_renders_deployment() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: None,
        issuer_ref: Some(crabka_operator::crd::CertManagerIssuerRef {
            name: "my-issuer".into(),
            kind: None,
            group: None,
        }),
        client_auth: None,
        client_ca_secret_name: None,
    });
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/certificates/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"cert-manager.io/v1","kind":"Certificate","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/secrets/sr1-sr-tls".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Secret","metadata":{"name":"sr1-sr-tls"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let joined = body["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        joined.contains("--tls-cert=/etc/sr/tls/tls.crt"),
        "joined: {joined}"
    );
    let vols = body["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap();
    let tls_vol = vols.iter().find(|v| v["name"] == "tls").unwrap();
    assert_eq!(tls_vol["secret"]["secretName"], "sr1-sr-tls");
}

#[tokio::test]
async fn bearer_jwks_renders_to_args() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into());
    cr.spec.authentication = Some(crabka_operator::crd::SchemaRegistryAuthn {
        require_auth: false,
        realm: None,
        basic: None,
        bearer: Some(crabka_operator::crd::BearerAuthn {
            mode: crabka_operator::crd::BearerMode::Jwks,
            principal_claim: None,
            jwks_endpoint_uri: Some("https://idp.example.com/jwks".into()),
            jwks_valid_issuer: Some("https://idp.example.com".into()),
            jwks_expected_audience: Some("kafka-sr".into()),
            jwks_tls_secret_name: None,
            jwks_principal_claim: Some("email".into()),
            jwks_refresh_ms: Some(30_000),
        }),
    });
    let rules = vec![
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr-headless".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr-headless"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Service","metadata":{"name":"sr1-sr"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/sr1-sr".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"Deployment","metadata":{"name":"sr1-sr"},"status":{"replicas":1,"readyReplicas":1}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/schemaregistries/sr1/status".into(),
            response: json_response(
                200,
                &serde_json::json!({"kind":"SchemaRegistry","metadata":{"name":"sr1"},"spec":{"replicas":1}}),
            ),
        },
    ];
    let state = MockState::new(rules);
    let client = mock_client(&state, NS);
    let ctx = Arc::new(fixture_ctx(client, NS));
    reconcile(Arc::new(cr), ctx).await.unwrap();

    let observed = state.take_observed();
    let dep = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/deployments/sr1-sr")
        })
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(dep.body()).unwrap();
    let joined = body["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    for needle in [
        "--bearer=jwks",
        "--bearer-jwks-endpoint-uri=https://idp.example.com/jwks",
        "--bearer-jwks-valid-issuer=https://idp.example.com",
        "--bearer-jwks-expected-audience=kafka-sr",
        "--bearer-jwks-principal-claim=email",
        "--bearer-jwks-refresh-ms=30000",
    ] {
        assert!(
            joined.contains(needle),
            "needle {needle:?}, joined: {joined}"
        );
    }
}
