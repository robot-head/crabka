use std::{sync::Arc, time::Duration};

use assert2::assert;
use crabka_gres_control::{
    RangeBoundary, RangeLayoutEntry, SqlUser, TenantId, TenantName, TenantRecord, TenantState,
};
use crabka_operator::{
    context::{GresControlLike, GresControlWriteError},
    controller::gres_tenant::reconcile,
    crd::{
        GresTenant, GresTenantRangeKey, GresTenantRangeSpec, GresTenantSpec, GresTenantStatus,
        SecretKeyRef,
    },
};
use crabka_security::scram::PgScramVerifier;
use http::Method;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::runtime::controller::Action;
use tokio::sync::Mutex;

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, MockState,
    fake_admin::{FakeAdminClient, RecordedCall, TopicState},
    fixture_ctx, json_response, mock_client,
};

fn fixture_password() -> String {
    std::process::id().to_string()
}

struct FakeGresControl {
    current: Mutex<Option<TenantRecord>>,
    upserts: Mutex<Vec<TenantRecord>>,
    deletes: Mutex<Vec<TenantName>>,
    manifests: Mutex<Vec<(TenantName, crabka_gres_control::FinalCheckpoint)>>,
    replace_failures_remaining: Mutex<u32>,
}

impl Default for FakeGresControl {
    fn default() -> Self {
        Self {
            current: Mutex::new(None),
            upserts: Mutex::new(Vec::new()),
            deletes: Mutex::new(Vec::new()),
            manifests: Mutex::new(Vec::new()),
            replace_failures_remaining: Mutex::new(0),
        }
    }
}

#[async_trait::async_trait]
impl GresControlLike for FakeGresControl {
    async fn get_tenant(
        &self,
        _tenant: &TenantName,
    ) -> Result<Option<TenantRecord>, GresControlWriteError> {
        Ok(self.current.lock().await.clone())
    }

    async fn replace_tenant_if_version(
        &self,
        record: &TenantRecord,
        expected_record_version: Option<u64>,
    ) -> Result<TenantRecord, GresControlWriteError> {
        let mut failures_remaining = self.replace_failures_remaining.lock().await;
        if *failures_remaining > 0 {
            *failures_remaining -= 1;
            return Err(crabka_gres_control::ControlError::InvalidField {
                field: "replace_tenant_if_version",
                reason: "injected replacement failure".into(),
            }
            .into());
        }
        drop(failures_remaining);
        let mut current = self.current.lock().await;
        let canonical_record = canonical_replacement(current.as_ref(), record.clone());
        canonical_record.ensure_valid()?;
        validate_replacement_version(canonical_record.record_version, expected_record_version)?;
        if current.as_ref() == Some(&canonical_record) {
            return Ok(canonical_record);
        }
        if current.as_ref().map(|record| record.record_version) != expected_record_version {
            return Err(crabka_gres_control::ControlError::RegistryVersionConflict {
                tenant: canonical_record.name.clone(),
                expected: expected_record_version.unwrap_or(0),
                actual: current.as_ref().map_or(0, |record| record.record_version),
            }
            .into());
        }
        self.upserts.lock().await.push(canonical_record.clone());
        *current = Some(canonical_record.clone());
        Ok(canonical_record)
    }

    async fn delete_tenant(&self, tenant: &TenantName) -> Result<(), GresControlWriteError> {
        self.deletes.lock().await.push(tenant.clone());
        Ok(())
    }

    async fn validate_final_checkpoint_manifest(
        &self,
        record: &TenantRecord,
    ) -> Result<(), GresControlWriteError> {
        let Some(checkpoint) = record.final_checkpoint.as_ref() else {
            return Err(
                crabka_operator::context::CheckpointManifestError::Verification(
                    "registry record has no final checkpoint".into(),
                )
                .into(),
            );
        };
        if self
            .manifests
            .lock()
            .await
            .iter()
            .any(|(tenant, manifest)| tenant == &record.name && manifest == checkpoint)
        {
            return Ok(());
        }
        Err(
            crabka_operator::context::CheckpointManifestError::Verification(
                "checkpoint is missing or does not match the registry record".into(),
            )
            .into(),
        )
    }
}

fn validate_replacement_version(
    record_version: u64,
    expected_record_version: Option<u64>,
) -> Result<(), crabka_gres_control::ControlError> {
    let expected_successor = expected_record_version.map_or(Ok(1), |version| {
        version
            .checked_add(1)
            .ok_or_else(|| crabka_gres_control::ControlError::InvalidField {
                field: "record_version",
                reason: "must not overflow when replaced".into(),
            })
    })?;
    if record_version == expected_successor {
        return Ok(());
    }
    Err(crabka_gres_control::ControlError::InvalidField {
        field: "record_version",
        reason: "must advance exactly once from the expected version".into(),
    })
}

fn canonical_replacement(
    current: Option<&TenantRecord>,
    mut incoming: TenantRecord,
) -> TenantRecord {
    let Some(current) = current else {
        return incoming;
    };
    incoming.wal_generation = incoming.wal_generation.max(current.wal_generation);
    for incoming_range in &mut incoming.ranges {
        if let Some(current_range) = current
            .ranges
            .iter()
            .find(|range| range.range_id == incoming_range.range_id)
        {
            incoming_range.wal_generation = incoming_range
                .wal_generation
                .max(current_range.wal_generation);
        }
    }
    incoming
}

fn ready_kafka_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": name, "namespace": namespace, "uid": "kafka-uid" },
        "spec": { "kafkaVersion": "0.1.1", "interBrokerListenerName": "PLAIN" },
        "status": {
            "conditions": [{ "type": "Ready", "status": "True", "reason": "Available", "message": "", "lastTransitionTime": "2026-05-17T00:00:00Z" }],
            "listeners": [{ "name": "PLAIN", "type": "internal", "bootstrapServers": format!("{name}-broker-headless.{namespace}.svc.cluster.local:9092"), "addresses": [] }],
        }
    })
}

fn gres_body(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Gres",
        "metadata": { "name": name, "namespace": namespace, "uid": "gres-uid" },
        "spec": {
            "kafkaCluster": "demo",
            "pgdog": { "replicas": 1, "listenPort": 6432, "adminSecretRef": { "name": "admin", "key": "password" } },
            "defaults": { "walReplication": 1 }
        }
    })
}

fn secret_body(name: &str, namespace: &str, password: &str) -> serde_json::Value {
    use base64::Engine as _;
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": namespace, "uid": "secret-uid" },
        "type": "Opaque",
        "data": { "password": base64::engine::general_purpose::STANDARD.encode(password.as_bytes()) }
    })
}

fn tenant() -> GresTenant {
    let mut tenant = GresTenant::new(
        "tenant-a",
        GresTenantSpec {
            gres: "fleet".into(),
            user: "alice".into(),
            password_secret_ref: SecretKeyRef {
                name: "pw".into(),
                key: "password".into(),
            },
            suspended: Some(false),
            resources: None,
            ranges: Vec::new(),
            overrides: None,
        },
    );
    tenant.metadata.namespace = Some("ns".into());
    tenant.metadata.uid = Some("tenant-uid".into());
    tenant.metadata.generation = Some(7);
    tenant.metadata.finalizers = Some(vec!["crabka.io/gres-tenant-finalizer".into()]);
    tenant
}

fn tenant_body() -> serde_json::Value {
    serde_json::to_value(tenant()).unwrap()
}

fn multi_range_tenant() -> GresTenant {
    let mut tenant = tenant();
    tenant.spec.ranges = vec![
        GresTenantRangeSpec {
            range_id: 0,
            end_key: Some(GresTenantRangeKey {
                table_id: 100,
                bucket: None,
                rowid: 0,
            }),
        },
        GresTenantRangeSpec {
            range_id: 1,
            end_key: Some(GresTenantRangeKey {
                table_id: 200,
                bucket: None,
                rowid: 0,
            }),
        },
        GresTenantRangeSpec {
            range_id: 2,
            end_key: None,
        },
    ];
    tenant
}

fn tenant_record(state: TenantState, generation: u64) -> TenantRecord {
    let tenant_name = TenantName::try_from("tenant-a").unwrap();
    let mut record = TenantRecord::new(
        9,
        TenantId::try_from("tenant-a").unwrap(),
        tenant_name,
        state,
        SqlUser::try_from("alice").unwrap(),
        PgScramVerifier::generate(&fixture_password(), 8192)
            .expect("fixture SCRAM verifier")
            .to_string(),
        1,
    )
    .unwrap();
    record.wal_generation = generation;
    record.ranges = vec![RangeLayoutEntry {
        range_id: 0,
        end_key: None,
        endpoint: "tenant-a-gres.ns.svc.cluster.local:7432".into(),
        wal_generation: generation,
        lifecycle: crabka_gres_control::RangeLifecycle::default(),
        retirement: None,
    }];
    record
}

fn multi_range_reconcile_rules() -> Vec<MockRule> {
    let mut rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: "/greses/fleet".into(),
            response: json_response(200, &gres_body("fleet", "ns")),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/demo".into(),
            response: json_response(200, &ready_kafka_body("demo", "ns")),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/secrets/pw".into(),
            response: json_response(200, &secret_body("pw", "ns", &fixture_password())),
        },
    ];
    rules.push(MockRule {
        method: Method::GET,
        path_substr: "/secrets/tenant-a-gres-range-tls".into(),
        response: json_response(
            404,
            &serde_json::json!({
                "apiVersion":"v1", "kind":"Status", "status":"Failure",
                "reason":"NotFound", "code":404
            }),
        ),
    });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: "/secrets/tenant-a-gres-range-tls".into(),
        response: json_response(
            200,
            &serde_json::json!({
                "apiVersion":"v1", "kind":"Secret",
                "metadata":{"name":"tenant-a-gres-range-tls","namespace":"ns"}
            }),
        ),
    });
    for name in [
        "tenant-a-gres-pg",
        "tenant-a-gres",
        "tenant-a-gres-r1",
        "tenant-a-gres-r2",
    ] {
        rules.push(MockRule { method: Method::PATCH, path_substr: format!("/services/{name}"), response: json_response(200, &serde_json::json!({"apiVersion":"v1","kind":"Service","metadata":{"name":name,"namespace":"ns"}})) });
    }
    for name in ["tenant-a-gres", "tenant-a-gres-r1", "tenant-a-gres-r2"] {
        rules.push(MockRule {
            method: Method::PATCH,
            path_substr: format!("/deployments/{name}"),
            response: json_response(200, &ready_deployment_body(name, 1)),
        });
        rules.push(MockRule {
            method: Method::GET,
            path_substr: format!("/deployments/{name}"),
            response: json_response(200, &ready_deployment_body(name, 1)),
        });
    }
    rules.push(MockRule { method: Method::PATCH, path_substr: "/networkpolicies/tenant-a-gres-range-policy".into(), response: json_response(200, &serde_json::json!({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"tenant-a-gres-range-policy","namespace":"ns"}})) });
    rules.push(MockRule {
        method: Method::PATCH,
        path_substr: "/grestenants/tenant-a/status".into(),
        response: json_response(200, &tenant_body()),
    });
    rules
}

fn ready_deployment_body(name: &str, replicas: i32) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": { "name": name, "namespace": "ns", "generation": 1 },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": { "app": name } },
            "template": { "metadata": { "labels": { "app": name } }, "spec": { "containers": [{ "name": "gres", "image": "gres" }] } }
        },
        "status": { "observedGeneration": 1, "availableReplicas": replicas }
    })
}

fn final_checkpoint(generation: u64) -> crabka_gres_control::FinalCheckpoint {
    crabka_gres_control::FinalCheckpoint {
        wal_generation: generation,
        covered_offset: 11,
        manifest_key: "gres/tenant-a/ckpt/manifest".into(),
        total_bytes: 64,
    }
}

#[tokio::test]
async fn fake_gres_control_matches_canonical_replace_and_retry_semantics() {
    let current = tenant_record(TenantState::Active, 4);
    let control = FakeGresControl {
        current: Mutex::new(Some(current)),
        ..Default::default()
    };
    let mut replacement = tenant_record(TenantState::Active, 1);
    replacement.record_version = 10;

    let stored = control
        .replace_tenant_if_version(&replacement, Some(9))
        .await
        .expect("immediate successor is accepted");
    assert!(stored.wal_generation == 4);
    assert!(
        control
            .replace_tenant_if_version(&replacement, Some(9))
            .await
            .expect("canonical stale retry is accepted")
            == stored
    );
    assert!(control.upserts.lock().await.len() == 1);

    replacement.record_version = 11;
    assert!(
        control
            .replace_tenant_if_version(&replacement, Some(9))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn multi_range_tenant_publishes_range_services_and_becomes_ready_after_all_deployments() {
    let mut rules = multi_range_reconcile_rules();
    rules.extend(multi_range_reconcile_rules());
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let control = Arc::new(FakeGresControl::default());
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    let action = reconcile(Arc::new(multi_range_tenant()), Arc::new(ctx))
        .await
        .unwrap();

    assert!(action == Action::requeue(Duration::from_secs(5)));
    let records = control.upserts.lock().await;
    assert!(records.len() == 1);
    assert!(records[0].ranges.len() == 3);
    assert!(records[0].ranges[0].endpoint == "tenant-a-gres.ns.svc.cluster.local:7432");
    assert!(records[0].ranges[1].endpoint == "tenant-a-gres-r1.ns.svc.cluster.local:7432");
    assert!(records[0].ranges[2].endpoint == "tenant-a-gres-r2.ns.svc.cluster.local:7432");
    drop(records);

    let observed = state.take_observed();
    let status = observed
        .iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains("/grestenants/tenant-a/status")
        })
        .expect("status patch captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["conditions"][0]["type"] == "Ready");
    assert!(body["status"]["conditions"][0]["status"] == "True");
    assert!(body["status"]["conditions"][0]["reason"] == "Ready");
    for service in [
        "tenant-a-gres-pg",
        "tenant-a-gres",
        "tenant-a-gres-r1",
        "tenant-a-gres-r2",
    ] {
        assert!(observed.iter().any(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .path()
                    .contains(&format!("/services/{service}"))
        }));
    }
    assert!(
        observed
            .iter()
            .filter(|request| request.method() == Method::GET
                && request.uri().path().contains("/deployments/"))
            .count()
            == 3
    );
}

#[tokio::test]
async fn deleting_multi_range_tenant_cleans_up_and_removes_its_finalizer() {
    let mut deleting_tenant = multi_range_tenant();
    deleting_tenant.metadata.deletion_timestamp = Some(Time(
        "2026-07-10T00:00:00Z"
            .parse()
            .expect("deletion timestamp parses"),
    ));
    let registry_policy = crabka_gres_control::RegistryPolicy::new(2, 15_001, 251, 501, 1_048_577)
        .expect("registry policy");
    let mut kafka = ready_kafka_body("demo", "ns");
    kafka["spec"]["gresRegistry"] = serde_json::json!({
        "replicationFactor": 2,
        "topicCreateTimeoutMs": 15001,
        "readerRetryBackoffMs": 251,
        "fetchMaxWaitMs": 501,
        "fetchPartitionMaxBytes": 1_048_577
    });
    let rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: "/greses/fleet".into(),
            response: json_response(200, &gres_body("fleet", "ns")),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/demo".into(),
            response: json_response(200, &kafka),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/grestenants/tenant-a".into(),
            response: json_response(200, &tenant_body()),
        },
    ];
    let state = MockState::new(rules);
    let ctx = fixture_ctx(mock_client(&state, "ns"), "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let control = Arc::new(FakeGresControl::default());
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test_with_policy(
        "ns",
        "demo",
        "demo-broker-headless.ns.svc.cluster.local:9092",
        registry_policy,
        control.clone(),
    )
    .await;

    let action = reconcile(Arc::new(deleting_tenant), Arc::new(ctx))
        .await
        .expect("deleting tenants bypass the multi-range rejection");

    assert!(action == Action::await_change());
    assert!(control.deletes.lock().await.as_slice() == [TenantName::try_from("tenant-a").unwrap()]);
    let calls = admin.lock().await.calls();
    assert!(calls.len() == 2);
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::AlterUserScramCredentials { upsertions, deletions } if upsertions.is_empty() && deletions.iter().any(|deletion| deletion.username == "gres-tenant-a"))));
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::DeleteAcls(filters) if filters.len() == 1 && filters[0].principal.as_deref() == Some("User:gres-tenant-a"))));

    let observed = state.take_observed();
    assert!(observed.len() == 3);
    let finalizer_patch = observed
        .iter()
        .find(|request| request.method() == Method::PATCH)
        .expect("finalizer removal patch captured");
    let body: serde_json::Value = serde_json::from_slice(finalizer_patch.body()).unwrap();
    assert!(body["metadata"]["finalizers"] == serde_json::json!([]));
}

#[tokio::test]
async fn dependency_failure_replaces_obsolete_multi_range_status() {
    let mut legacy_tenant = tenant();
    legacy_tenant.status = Some(GresTenantStatus {
        conditions: vec![crabka_operator::crd::KafkaCondition {
            type_: "Ready".into(),
            status: "False".into(),
            reason: "MultiRangeUnsupported".into(),
            message: "legacy registry layout has multiple ranges".into(),
            last_transition_time: "2026-07-10T00:00:00Z".into(),
        }],
        ..Default::default()
    });
    let state = MockState::new(vec![
        MockRule {
            method: Method::GET,
            path_substr: "/greses/fleet".into(),
            response: json_response(
                404,
                &serde_json::json!({
                    "apiVersion":"v1", "kind":"Status", "metadata":{}, "status":"Failure",
                    "message":"not found", "reason":"NotFound", "code":404
                }),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/grestenants/tenant-a/status".into(),
            response: json_response(200, &tenant_body()),
        },
    ]);
    let ctx = fixture_ctx(mock_client(&state, "ns"), "ns");

    let action = reconcile(Arc::new(legacy_tenant), Arc::new(ctx))
        .await
        .expect("missing Gres is represented by a dependency requeue");
    assert!(action == Action::requeue(Duration::from_secs(30)));

    let observed = state.take_observed();
    let status = observed
        .iter()
        .find(|request| request.uri().path().ends_with("/status"))
        .expect("dependency status patch");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["conditions"][0]["reason"] == "GresNotFound");
}

#[tokio::test]
async fn successful_single_range_registry_read_allows_a_later_failure_to_replace_legacy_status() {
    let mut legacy_tenant = tenant();
    legacy_tenant.status = Some(GresTenantStatus {
        conditions: vec![crabka_operator::crd::KafkaCondition {
            type_: "Ready".into(),
            status: "False".into(),
            reason: "MultiRangeUnsupported".into(),
            message: "legacy registry layout has multiple ranges".into(),
            last_transition_time: "2026-07-10T00:00:00Z".into(),
        }],
        ..Default::default()
    });
    let mut rules = tenant_reconcile_rules();
    rules[2].response = json_response(
        404,
        &serde_json::json!({
            "apiVersion":"v1", "kind":"Status", "metadata":{}, "status":"Failure",
            "message":"not found", "reason":"NotFound", "code":404
        }),
    );
    let state = MockState::new(rules);
    let ctx = fixture_ctx(mock_client(&state, "ns"), "ns");
    ctx.insert_admin_client_for_test(
        "demo",
        Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new())),
    )
    .await;
    ctx.insert_gres_control_for_test(
        "ns",
        "demo",
        Arc::new(FakeGresControl {
            current: Mutex::new(Some(tenant_record(TenantState::Active, 0))),
            ..Default::default()
        }),
    )
    .await;

    reconcile(Arc::new(legacy_tenant), Arc::new(ctx))
        .await
        .expect_err("missing password secret fails after a single-range registry read");

    let observed = state.take_observed();
    let status = observed
        .iter()
        .find(|request| request.uri().path().ends_with("/status"))
        .expect("status patch captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["conditions"][0]["reason"] == "ReconcileFailed");
}

fn tenant_reconcile_rules() -> Vec<MockRule> {
    vec![
        MockRule {
            method: Method::GET,
            path_substr: "/greses/fleet".into(),
            response: json_response(200, &gres_body("fleet", "ns")),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/demo".into(),
            response: json_response(200, &ready_kafka_body("demo", "ns")),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/secrets/pw".into(),
            response: json_response(200, &secret_body("pw", "ns", &fixture_password())),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/tenant-a-gres".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"tenant-a-gres","namespace":"ns"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/tenant-a-gres".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"tenant-a-gres","namespace":"ns"}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/deployments/tenant-a-gres".into(),
            response: json_response(200, &ready_deployment_body("tenant-a-gres", 1)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/networkpolicies/tenant-a-gres-range-policy".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"tenant-a-gres-range-policy","namespace":"ns"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/grestenants/tenant-a/status".into(),
            response: json_response(200, &tenant_body()),
        },
    ]
}

#[tokio::test]
async fn reconciles_topics_scram_acls_records_workload_and_status() {
    let rules = tenant_reconcile_rules();
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let control = Arc::new(FakeGresControl::default());
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    let action = reconcile(Arc::new(tenant()), Arc::new(ctx)).await.unwrap();
    assert!(action == Action::requeue(Duration::from_secs(5)));

    let calls = admin.lock().await.calls();
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::CreateTopics(specs) if specs.iter().any(|spec| spec.name == "__gres_wal.tenant-a.r0") && specs.iter().any(|spec| spec.name == "__gres_cfg.tenant-a") && !specs.iter().any(|spec| spec.name == crabka_gres_control::TENANT_REGISTRY_TOPIC))));
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::AlterUserScramCredentials { upsertions, .. } if upsertions.iter().any(|upsert| upsert.username == "gres-tenant-a"))));
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::CreateAcls(acls) if acls.iter().any(|acl| acl.resource_name == "__gres_wal.tenant-a" && acl.pattern_type == crabka_client_admin::PatternType::Prefixed && acl.principal == "User:gres-tenant-a") && acls.iter().any(|acl| acl.resource_name == "__gres.tenant-a" && acl.pattern_type == crabka_client_admin::PatternType::Prefixed) && !acls.iter().any(|acl| acl.resource_name == "__gres_tenants"))));
    let upserts = control.upserts.lock().await;
    assert!(upserts.len() == 1);
    assert!(!upserts[0].scram_verifier.contains("hunter2"));
    assert!(upserts[0].record_version == 1);
    drop(upserts);
    let observed = state.take_observed();
    let deployment = observed
        .iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request.uri().path().contains("/deployments/tenant-a-gres")
        })
        .expect("compute deployment patch");
    let deployment: serde_json::Value =
        serde_json::from_slice(deployment.body()).expect("deployment JSON");
    let args = deployment["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array()
        .expect("compute args");
    for pair in [
        ["--registry-replication-factor", "1"],
        ["--registry-topic-create-timeout-ms", "15000"],
        ["--registry-reader-retry-backoff-ms", "250"],
        ["--registry-fetch-max-wait-ms", "500"],
        ["--registry-fetch-partition-max-bytes", "1048576"],
    ] {
        assert!(
            args.windows(2).any(|window| {
                window[0].as_str() == Some(pair[0]) && window[1].as_str() == Some(pair[1])
            }),
            "missing {pair:?}: {args:?}"
        );
    }
    let status = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/grestenants/tenant-a/status")
        })
        .expect("status patch captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["registryVersion"] == 1);
}

#[tokio::test]
async fn repeated_reconcile_preserves_scram_and_does_not_replace_the_registry_record() {
    let mut rules = tenant_reconcile_rules();
    rules.extend(tenant_reconcile_rules());
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let control = Arc::new(FakeGresControl::default());
    ctx.insert_admin_client_for_test("demo", admin).await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    reconcile(Arc::new(tenant()), Arc::new(ctx.clone()))
        .await
        .unwrap();
    let first = control.current.lock().await.clone().expect("first record");

    reconcile(Arc::new(tenant()), Arc::new(ctx)).await.unwrap();
    let current = control
        .current
        .lock()
        .await
        .clone()
        .expect("current record");

    assert!(current == first);
    assert!(control.upserts.lock().await.len() == 1);
}

#[tokio::test]
async fn suspended_registry_state_parks_wal_and_scales_compute_to_zero() {
    let state = MockState::new(tenant_reconcile_rules());
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    admin.lock().await.add_topic(
        "__gres_wal.tenant-a.r0.g0000000004",
        TopicState {
            partitions: 1,
            replicas: 1,
            ..Default::default()
        },
    );
    let mut suspended = tenant_record(TenantState::Suspended, 4);
    suspended.final_checkpoint = Some(final_checkpoint(4));
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(suspended)),
        manifests: Mutex::new(vec![(
            TenantName::try_from("tenant-a").unwrap(),
            final_checkpoint(4),
        )]),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    reconcile(Arc::new(tenant()), Arc::new(ctx)).await.unwrap();

    let calls = admin.lock().await.calls();
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::DeleteTopics(names) if names == &vec!["__gres_wal.tenant-a.r0.g0000000004".to_string()])));
    let upserts = control.upserts.lock().await;
    assert!(upserts.len() == 2);
    assert!(upserts[0].state == TenantState::Parking);
    assert!(upserts[0].wal_generation == 5);
    assert!(upserts[1].state == TenantState::Suspended);
    assert!(upserts[1].final_checkpoint == Some(final_checkpoint(4)));
    let observed = state.take_observed();
    let deployment = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/deployments/tenant-a-gres")
        })
        .expect("deployment patch captured");
    let body: serde_json::Value = serde_json::from_slice(deployment.body()).unwrap();
    assert!(body["spec"]["replicas"] == 0);
    let status = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/grestenants/tenant-a/status")
        })
        .expect("status patch captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["lifecyclePhase"] == "suspended");
}

#[tokio::test]
async fn range_parking_deletes_only_predecessor_generation_and_keeps_tenant_active() {
    let mut rules = multi_range_reconcile_rules();
    rules.extend(multi_range_reconcile_rules());
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    for range_id in 0..=2 {
        admin.lock().await.add_topic(
            &format!("__gres_wal.tenant-a.r{range_id}.g0000000004"),
            TopicState {
                partitions: 1,
                replicas: 1,
                ..Default::default()
            },
        );
    }
    let mut current = tenant_record(TenantState::Active, 4);
    current.ranges = vec![
        RangeLayoutEntry {
            range_id: 0,
            end_key: Some(RangeBoundary::table_start(10)),
            endpoint: "tenant-a-gres.ns.svc.cluster.local:7432".into(),
            wal_generation: 4,
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 1,
            end_key: Some(RangeBoundary::table_start(20)),
            endpoint: "tenant-a-gres-r1.ns.svc.cluster.local:7432".into(),
            wal_generation: 4,
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        },
        RangeLayoutEntry {
            range_id: 2,
            end_key: None,
            endpoint: "tenant-a-gres-r2.ns.svc.cluster.local:7432".into(),
            wal_generation: 4,
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        },
    ];
    current = current
        .clone()
        .publish_split_target_with_retirement(
            "split-7",
            0,
            4,
            crabka_gres_control::RangeRetirementCheckpoint {
                manifest_key: "tenant-a/r0/g4/manifest".into(),
                covered_offset: 10,
                barrier_offset: 12,
                tail_sha256: "tail".into(),
                marker_digest: "markers".into(),
            },
            current.ranges.clone(),
        )
        .expect("parking intent");
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(current)),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    reconcile(Arc::new(multi_range_tenant()), Arc::new(ctx.clone()))
        .await
        .expect("range parking reconcile");
    reconcile(Arc::new(multi_range_tenant()), Arc::new(ctx))
        .await
        .expect("parked range restart convergence");

    let calls = admin.lock().await.calls();
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::DeleteTopics(names) if names == &vec!["__gres_wal.tenant-a.r0.g0000000004".to_string()])));
    assert!(!calls.iter().any(|call| matches!(call, RecordedCall::DeleteTopics(names) if names.iter().any(|name| name.contains(".r1.") || name.contains(".r2.")))));
    assert_eq!(calls.iter().filter(|call| matches!(call, RecordedCall::DeleteTopics(names) if names == &vec!["__gres_wal.tenant-a.r0.g0000000004".to_string()])).count(), 1);
    let stored = control.current.lock().await.clone().expect("stored record");
    assert_eq!(stored.state, TenantState::Active);
    assert!(
        stored
            .ranges
            .iter()
            .all(|range| range.lifecycle == crabka_gres_control::RangeLifecycle::Serving)
    );
    assert_eq!(stored.range_retirements.len(), 1);
    assert_eq!(
        stored.range_retirements[0].phase,
        crabka_gres_control::RangeRetirementPhase::Parked
    );
}

#[tokio::test]
async fn parking_waits_for_wal_metadata_to_confirm_asynchronous_deletion() {
    let mut rules = tenant_reconcile_rules();
    rules.extend(tenant_reconcile_rules());
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    admin.lock().await.add_topic(
        "__gres_wal.tenant-a.r0.g0000000004",
        TopicState {
            partitions: 1,
            replicas: 1,
            ..Default::default()
        },
    );
    admin.lock().await.retain_topics_after_delete_ack();
    let mut suspended = tenant_record(TenantState::Suspended, 4);
    suspended.final_checkpoint = Some(final_checkpoint(4));
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(suspended)),
        manifests: Mutex::new(vec![(
            TenantName::try_from("tenant-a").unwrap(),
            final_checkpoint(4),
        )]),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    let action = reconcile(Arc::new(tenant()), Arc::new(ctx.clone()))
        .await
        .expect("DeleteTopics acknowledgement is accepted");

    assert!(action == Action::requeue(Duration::from_secs(5)));
    assert!(control.current.lock().await.as_ref().unwrap().state == TenantState::Parking);
    let calls = admin.lock().await.calls();
    assert!(!calls.iter().any(|call| matches!(call, RecordedCall::CreateTopics(specs) if specs.iter().any(|spec| spec.name == "__gres_wal.tenant-a.r0.g0000000004"))));
    drop(calls);
    let observed = state.take_observed();
    let deployment = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/deployments/tenant-a-gres")
        })
        .expect("pending WAL deletion scales compute down");
    let body: serde_json::Value = serde_json::from_slice(deployment.body()).unwrap();
    assert!(body["spec"]["replicas"] == 0);

    admin
        .lock()
        .await
        .remove_topic("__gres_wal.tenant-a.r0.g0000000004");
    reconcile(Arc::new(tenant()), Arc::new(ctx))
        .await
        .expect("absent WAL metadata completes parking");

    assert!(control.current.lock().await.as_ref().unwrap().state == TenantState::Suspended);
}

#[tokio::test]
async fn failed_parking_intent_replacement_keeps_wal_until_retry_then_converges_once() {
    let mut rules = tenant_reconcile_rules();
    rules.extend(tenant_reconcile_rules());
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    admin.lock().await.add_topic(
        "__gres_wal.tenant-a.r0.g0000000004",
        TopicState {
            partitions: 1,
            replicas: 1,
            ..Default::default()
        },
    );
    let mut suspended = tenant_record(TenantState::Suspended, 4);
    suspended.final_checkpoint = Some(final_checkpoint(4));
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(suspended)),
        manifests: Mutex::new(vec![(
            TenantName::try_from("tenant-a").unwrap(),
            final_checkpoint(4),
        )]),
        replace_failures_remaining: Mutex::new(1),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    reconcile(Arc::new(tenant()), Arc::new(ctx.clone()))
        .await
        .expect_err("parking intent replacement fails before WAL deletion");
    assert!(
        admin
            .lock()
            .await
            .calls()
            .iter()
            .all(|call| !matches!(call, RecordedCall::DeleteTopics(_)))
    );
    assert!(
        control
            .current
            .lock()
            .await
            .as_ref()
            .unwrap()
            .wal_generation
            == 4
    );

    reconcile(Arc::new(tenant()), Arc::new(ctx))
        .await
        .expect("retry persists the intent and deletes WAL");
    assert!(
        control
            .current
            .lock()
            .await
            .as_ref()
            .unwrap()
            .wal_generation
            == 5
    );
    assert!(
        admin
            .lock()
            .await
            .calls()
            .iter()
            .filter(|call| matches!(call, RecordedCall::DeleteTopics(_)))
            .count()
            == 1
    );
}

#[tokio::test]
async fn failed_wal_deletion_keeps_parking_intent_and_retry_converges() {
    let mut rules = tenant_reconcile_rules();
    rules.extend(tenant_reconcile_rules());
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    admin.lock().await.add_topic(
        "__gres_wal.tenant-a.r0.g0000000004",
        TopicState {
            partitions: 1,
            replicas: 1,
            ..Default::default()
        },
    );
    admin
        .lock()
        .await
        .inject_delete_topics_broker_error(1, "BROKER_NOT_AVAILABLE", None);
    let mut suspended = tenant_record(TenantState::Suspended, 4);
    suspended.final_checkpoint = Some(final_checkpoint(4));
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(suspended)),
        manifests: Mutex::new(vec![(
            TenantName::try_from("tenant-a").unwrap(),
            final_checkpoint(4),
        )]),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    reconcile(Arc::new(tenant()), Arc::new(ctx.clone()))
        .await
        .expect_err("WAL deletion fails after parking intent persists");
    assert!(control.current.lock().await.as_ref().unwrap().state == TenantState::Parking);
    admin.lock().await.injected.lock().unwrap().delete_topics = None;

    reconcile(Arc::new(tenant()), Arc::new(ctx))
        .await
        .expect("parking retry deletes WAL and completes suspension");
    assert!(control.current.lock().await.as_ref().unwrap().state == TenantState::Suspended);
    assert!(
        admin
            .lock()
            .await
            .calls()
            .iter()
            .filter(|call| matches!(call, RecordedCall::DeleteTopics(_)))
            .count()
            == 2
    );
}

#[tokio::test]
async fn resume_request_starts_current_generation_without_deleting_previous_generation() {
    let state = MockState::new(tenant_reconcile_rules());
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    admin.lock().await.add_topic(
        "__gres_wal.tenant-a.r0.g0000000004",
        TopicState {
            partitions: 1,
            replicas: 1,
            ..Default::default()
        },
    );
    let resume_requested = tenant_record(TenantState::ResumeRequested, 5);
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(resume_requested.clone())),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    let action = reconcile(Arc::new(tenant()), Arc::new(ctx))
        .await
        .expect("resume request remains fenced while the WAL topic remains in metadata");

    assert!(action == Action::requeue(Duration::from_secs(5)));
    assert!(control.current.lock().await.as_ref() == Some(&resume_requested));
    assert!(control.upserts.lock().await.is_empty());
    assert!(
        admin
            .lock()
            .await
            .topics
            .lock()
            .unwrap()
            .contains_key("__gres_wal.tenant-a.r0.g0000000004")
    );
    let calls = admin.lock().await.calls();
    assert!(
        !calls
            .iter()
            .any(|call| matches!(call, RecordedCall::DeleteTopics(_)))
    );
    assert!(!calls.iter().any(
        |call| matches!(call, RecordedCall::CreateTopics(specs) if specs.iter().any(|spec| spec.name == "__gres_wal.tenant-a.r0.g0000000004"))
    ));
    drop(calls);
    let observed = state.take_observed();
    let deployments: Vec<_> = observed
        .iter()
        .filter(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains("/deployments/tenant-a-gres")
        })
        .collect();
    assert!(deployments.len() == 1);
    for deployment in deployments {
        let body: serde_json::Value = serde_json::from_slice(deployment.body()).unwrap();
        assert!(body["spec"]["replicas"] == 1);
    }
}

#[tokio::test]
async fn suspended_tenant_failure_preserves_lifecycle_without_reactivating_routing() {
    let mut tenant = tenant();
    tenant.status = Some(GresTenantStatus {
        lifecycle_phase: Some("suspended".into()),
        ..Default::default()
    });
    let mut rules = tenant_reconcile_rules();
    rules[0].response = json_response(
        404,
        &serde_json::json!({
            "apiVersion":"v1",
            "kind":"Status",
            "metadata":{},
            "status":"Failure",
            "message":"not found",
            "reason":"NotFound",
            "code":404
        }),
    );
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");

    reconcile(Arc::new(tenant), Arc::new(ctx)).await.unwrap();

    let observed = state.take_observed();
    let status = observed
        .iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains("/grestenants/tenant-a/status")
        })
        .expect("status patch captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["lifecyclePhase"] == "suspended");
    assert!(body["status"]["ready"] == false);
    assert!(!observed.iter().any(|request| {
        request
            .uri()
            .to_string()
            .contains("/deployments/tenant-a-gres")
    }));
}

#[tokio::test]
async fn registry_suspension_is_preserved_when_a_later_dependency_fails() {
    let mut rules = tenant_reconcile_rules();
    rules[2].response = json_response(
        404,
        &serde_json::json!({
            "apiVersion":"v1",
            "kind":"Status",
            "metadata":{},
            "status":"Failure",
            "message":"not found",
            "reason":"NotFound",
            "code":404
        }),
    );
    let state = MockState::new(rules);
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let mut suspended = tenant_record(TenantState::Suspended, 4);
    suspended.final_checkpoint = Some(final_checkpoint(4));
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(suspended)),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test(
        "demo",
        Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new())),
    )
    .await;
    ctx.insert_gres_control_for_test("ns", "demo", control)
        .await;

    reconcile(Arc::new(tenant()), Arc::new(ctx))
        .await
        .expect_err("missing password secret fails after registry read");

    let observed = state.take_observed();
    let status = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/grestenants/tenant-a/status")
        })
        .expect("status patch captured");
    let body: serde_json::Value = serde_json::from_slice(status.body()).unwrap();
    assert!(body["status"]["lifecyclePhase"] == "suspended");
    assert!(body["status"]["registryVersion"] == 9);
    assert!(body["status"]["ready"] == false);
}

#[tokio::test]
async fn missing_final_checkpoint_manifest_blocks_suspended_parking() {
    let state = MockState::new(tenant_reconcile_rules());
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    admin.lock().await.add_topic(
        "__gres_wal.tenant-a.r0.g0000000004",
        TopicState {
            partitions: 1,
            replicas: 1,
            ..Default::default()
        },
    );
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(tenant_record(TenantState::Suspended, 4))),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    let error = reconcile(Arc::new(tenant()), Arc::new(ctx))
        .await
        .expect_err("missing manifest blocks parking");

    assert!(format!("{error}").contains("durable checkpoint manifest"));
    let calls = admin.lock().await.calls();
    assert!(
        !calls
            .iter()
            .any(|call| matches!(call, RecordedCall::DeleteTopics(_)))
    );
    assert!(control.upserts.lock().await.is_empty());
}

#[tokio::test]
async fn parked_tenant_reconciles_again_without_revalidating_stale_checkpoint() {
    let state = MockState::new(tenant_reconcile_rules());
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    admin.lock().await.add_topic(
        "__gres_wal.tenant-a.r0.g0000000004",
        TopicState {
            partitions: 1,
            replicas: 1,
            ..Default::default()
        },
    );
    let mut suspended = tenant_record(TenantState::Suspended, 4);
    suspended.final_checkpoint = Some(final_checkpoint(4));
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(suspended)),
        manifests: Mutex::new(vec![(
            TenantName::try_from("tenant-a").unwrap(),
            final_checkpoint(4),
        )]),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    reconcile(Arc::new(tenant()), Arc::new(ctx.clone()))
        .await
        .unwrap();
    control.manifests.lock().await.clear();
    state.rules.lock().unwrap().extend(tenant_reconcile_rules());
    reconcile(Arc::new(tenant()), Arc::new(ctx)).await.unwrap();

    assert!(control.upserts.lock().await.len() == 2);
    assert!(
        admin
            .lock()
            .await
            .calls()
            .iter()
            .filter(|call| matches!(call, RecordedCall::DeleteTopics(_)))
            .count()
            == 1
    );
}

#[tokio::test]
async fn fake_manifest_verifier_accepts_only_the_exact_durable_checkpoint() {
    let mut record = tenant_record(TenantState::Suspended, 4);
    record.final_checkpoint = Some(final_checkpoint(4));
    let tenant = TenantName::try_from("tenant-a").unwrap();
    let control = FakeGresControl {
        manifests: Mutex::new(vec![(tenant.clone(), final_checkpoint(4))]),
        ..Default::default()
    };

    control
        .validate_final_checkpoint_manifest(&record)
        .await
        .expect("exact durable checkpoint is accepted");

    let mut stale = final_checkpoint(4);
    stale.wal_generation = 3;
    *control.manifests.lock().await = vec![(tenant.clone(), stale)];
    assert!(
        control
            .validate_final_checkpoint_manifest(&record)
            .await
            .is_err()
    );

    let mut malformed = final_checkpoint(4);
    malformed.manifest_key.clear();
    *control.manifests.lock().await = vec![(tenant.clone(), malformed)];
    assert!(
        control
            .validate_final_checkpoint_manifest(&record)
            .await
            .is_err()
    );

    control.manifests.lock().await.clear();
    assert!(
        control
            .validate_final_checkpoint_manifest(&record)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn resume_requested_ensures_wal_and_scales_compute_to_one() {
    let state = MockState::new(tenant_reconcile_rules());
    let client = mock_client(&state, "ns");
    let ctx = fixture_ctx(client, "ns");
    let admin = Arc::new(tokio::sync::Mutex::new(FakeAdminClient::new()));
    let control = Arc::new(FakeGresControl {
        current: Mutex::new(Some(tenant_record(TenantState::ResumeRequested, 5))),
        ..Default::default()
    });
    ctx.insert_admin_client_for_test("demo", admin.clone())
        .await;
    ctx.insert_gres_control_for_test("ns", "demo", control.clone())
        .await;

    reconcile(Arc::new(tenant()), Arc::new(ctx)).await.unwrap();

    let calls = admin.lock().await.calls();
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::CreateTopics(specs) if specs.iter().any(|spec| spec.name == "__gres_wal.tenant-a.r0.g0000000005"))));
    assert!(
        !calls
            .iter()
            .any(|call| matches!(call, RecordedCall::DeleteTopics(_)))
    );
    let observed = state.take_observed();
    let deployment = observed
        .iter()
        .find(|request| {
            request.method() == Method::PATCH
                && request
                    .uri()
                    .to_string()
                    .contains("/deployments/tenant-a-gres")
        })
        .expect("deployment patch captured");
    let body: serde_json::Value = serde_json::from_slice(deployment.body()).unwrap();
    assert!(body["spec"]["replicas"] == 1);
    let resumed = control.current.lock().await.clone().unwrap();
    assert!(resumed.state == TenantState::ResumeRequested);
    assert!(resumed.record_version == 9);
}
