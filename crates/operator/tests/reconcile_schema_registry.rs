//! Reconcile-level tests for the `SchemaRegistry` controller.
//!
//! These assert the kube-side request sequence the reconciler issues
//! (Kafka GET, Service/Deployment SSA applies, status patch) and the
//! rendered Deployment container args / env / Secret mounts.

use assert2::assert;
use std::sync::Arc;

use crabka_operator::controller::schema_registry::reconcile;
use crabka_operator::crd::{SchemaRegistry, SchemaRegistrySpec};
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
#[allow(clippy::too_many_lines)] // the typed-security spec + mock-rule enumeration make the length inherent
async fn full_security_fields_render_to_args_and_mounts() {
    let mut cr = sr("sr1", Some(CLUSTER));
    cr.spec.bootstrap_servers = Some("ext:9092".into()); // skip the Kafka GET
    cr.spec.tls = Some(crabka_operator::crd::SchemaRegistryTls {
        secret_name: "sr-tls".into(),
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
    assert!(joined.contains("--tls-cert=/etc/sr/tls/tls.crt"));
    assert!(joined.contains("--tls-client-auth=required"));
    assert!(joined.contains("--tls-client-ca=/etc/sr/client-ca/ca.crt"));
    assert!(joined.contains("--require-auth"));
    assert!(joined.contains("--basic-auth-file=/etc/sr/basic/users"));
    assert!(joined.contains("--authz"));
    assert!(joined.contains("--super-user=User:admin"));
    assert!(joined.contains("--acl-refresh-secs=15"));
    // Mounts present for tls/client-ca/basic.
    let mounts = c["volumeMounts"].as_array().unwrap();
    let mount_paths: Vec<&str> = mounts
        .iter()
        .map(|m| m["mountPath"].as_str().unwrap())
        .collect();
    assert!(mount_paths.contains(&"/etc/sr/tls"));
    assert!(mount_paths.contains(&"/etc/sr/client-ca"));
    assert!(mount_paths.contains(&"/etc/sr/basic"));
}
