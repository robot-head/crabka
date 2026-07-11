//! Reconcile-level tests for the `SchemaRegistry` controller.
//!
//! These assert the kube-side request sequence the reconciler issues
//! (Kafka GET, Service/Deployment SSA applies, status patch) and the
//! rendered Deployment container args / env / Secret mounts.

use std::sync::Arc;

use crabka_operator::{
    controller::schema_registry::reconcile,
    crd::{SchemaRegistry, SchemaRegistrySpec},
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
    assert2::assert!(
        !observed
            .iter()
            .any(|r| r.uri().to_string().contains("/services/"))
    );
    assert2::assert!(
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
    assert2::assert!(kr["status"].as_str() == Some("False"));
    assert2::assert!(kr["reason"].as_str() == Some("KafkaNotReady"));
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
        assert2::assert!(joined.contains(needle));
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
    assert2::assert!(ready["status"].as_str() == Some("False"));
    assert2::assert!(ready["reason"].as_str() == Some("MissingClusterLabel"));
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
    assert2::assert!(
        joined.contains("--bootstrap-servers=demo-broker-headless.default.svc.cluster.local:9092")
    );
    assert2::assert!(joined.contains("--schemas-topic-rf=1"));
    // advertised-url env uses $(POD_NAME) interpolation.
    let env = body["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .unwrap();
    let adv = env
        .iter()
        .find(|e| e["name"] == "SCHEMA_REGISTRY_ADVERTISED_URL")
        .unwrap();
    assert2::assert!(
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
    assert2::assert!(ready["status"] == "True");
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // the typed-security spec + mock-rule enumeration make the length inherent
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
        assert2::assert!(joined.contains(needle));
    }
    // Mounts present for tls/client-ca/basic.
    let mounts = c["volumeMounts"].as_array().unwrap();
    let mount_paths: Vec<&str> = mounts
        .iter()
        .map(|m| m["mountPath"].as_str().unwrap())
        .collect();
    for path in ["/etc/sr/tls", "/etc/sr/client-ca", "/etc/sr/basic"] {
        assert2::assert!(mount_paths.contains(&path));
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
    assert2::assert!(!joined.contains("--kafka-security-protocol"));
    assert2::assert!(!joined.contains("--kafka-sasl-mechanism"));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
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
        assert2::assert!(joined.contains(needle));
    }
    // Env: SASL creds via secretKeyRef
    let env = c["env"].as_array().unwrap();
    let sasl_user = env
        .iter()
        .find(|e| e["name"] == "SCHEMA_REGISTRY_KAFKA_SASL_USERNAME")
        .unwrap();
    let sasl_pass = env
        .iter()
        .find(|e| e["name"] == "SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD")
        .unwrap();
    assert2::assert!(
        &sasl_user["valueFrom"]["secretKeyRef"]
            == &serde_json::json!({"name": "kafka-creds", "key": "username"})
    );
    assert2::assert!(
        &sasl_pass["valueFrom"]["secretKeyRef"]
            == &serde_json::json!({"name": "kafka-creds", "key": "password"})
    );
    // Volume + mount for kafka-tls CA
    let vols = body["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap();
    assert2::assert!(vols.iter().any(|v| v["name"] == "kafka-tls"));
    let mounts = c["volumeMounts"].as_array().unwrap();
    assert2::assert!(mounts.iter().any(|m| m["mountPath"] == "/etc/sr/kafka-tls"));
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
    assert2::assert!(
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
    assert2::assert!(ready["reason"] == "InvalidSpec");
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
    assert2::assert!(observed.iter().any(
        |r| r.method() == Method::PATCH && r.uri().to_string().contains("/certificates/sr1-sr")
    ));
    assert2::assert!(
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
    assert2::assert!(ready["reason"] == "WaitingForCert");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
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
    assert2::assert!(joined.contains("--tls-cert=/etc/sr/tls/tls.crt"));
    let vols = body["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap();
    let tls_vol = vols.iter().find(|v| v["name"] == "tls").unwrap();
    assert2::assert!(tls_vol["secret"]["secretName"] == "sr1-sr-tls");
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
        assert2::assert!(joined.contains(needle));
    }
}
