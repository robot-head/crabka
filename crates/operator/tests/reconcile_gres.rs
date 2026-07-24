use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use assert2::assert;
use crabka_operator::{
    context::{PgdogAdminError, PgdogAdminLike, PgdogExpectedRoute, PgdogReloadRequest},
    controller::{
        common::ReconcileError,
        gres::{error_policy, reconcile, tenant_endpoint, tenant_to_gres_refs},
    },
    crd::{
        Gres, GresActivatorSpec, GresBalancerGoal, GresBalancerGoals, GresBalancerOperationKind,
        GresBalancerPlanSnapshot, GresBalancerRegistryLayout, GresBalancerSpec,
        GresBalancerThresholds, GresSpec, GresTenant, GresTenantSpec, PgdogPoolerModeSpec,
        PgdogSpec, SecretKeyRef, SecretRef, TenantDefaults,
    },
};
use http::Method;

#[path = "shared/mod.rs"]
mod shared;

use shared::{MockRule, MockState, fixture_ctx, json_response, mock_client};

#[derive(Debug)]
struct FakePgdogAdmin {
    call_times: Mutex<Vec<Instant>>,
    delay: Duration,
    outcomes: Mutex<Vec<bool>>,
    requests: Mutex<Vec<Vec<PgdogReloadRequest>>>,
}

impl FakePgdogAdmin {
    fn new(outcomes: Vec<bool>) -> Arc<Self> {
        Arc::new(Self {
            call_times: Mutex::new(Vec::new()),
            delay: Duration::ZERO,
            outcomes: Mutex::new(outcomes),
            requests: Mutex::new(Vec::new()),
        })
    }

    fn with_delay(outcomes: Vec<bool>, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            call_times: Mutex::new(Vec::new()),
            delay,
            outcomes: Mutex::new(outcomes),
            requests: Mutex::new(Vec::new()),
        })
    }

    fn requests(&self) -> Vec<Vec<PgdogReloadRequest>> {
        self.requests.lock().unwrap().clone()
    }

    fn call_times(&self) -> Vec<Instant> {
        self.call_times.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl PgdogAdminLike for FakePgdogAdmin {
    async fn reload_and_database_views_match(
        &self,
        requests: &[PgdogReloadRequest],
    ) -> Result<bool, PgdogAdminError> {
        self.call_times.lock().unwrap().push(Instant::now());
        tokio::time::sleep(self.delay).await;
        self.requests.lock().unwrap().push(requests.to_vec());
        Ok(self.outcomes.lock().unwrap().remove(0))
    }
}

fn gres() -> Gres {
    let mut obj = Gres::new(
        "fleet",
        GresSpec {
            kafka_cluster: "demo".into(),
            pgdog: PgdogSpec {
                image: None,
                replicas: 1,
                listen_port: 6432,
                tls_secret_ref: None,
                admin_secret_ref: SecretKeyRef {
                    name: "admin".into(),
                    key: "password".into(),
                },
                pooler_mode: None,
                connect_attempts: None,
                idle_timeout_ms: None,
                suspension_idle_timeout_ms: None,
                server_lifetime_ms: None,
                readiness_probe_period_seconds: None,
                direct_bootstrap_grace_ms: None,
            },
            activator: None,
            defaults: None,
            balancer: Some(GresBalancerSpec {
                enabled: true,
                goals: GresBalancerGoals {
                    disabled_goals: vec![GresBalancerGoal::LoadSkew],
                },
                thresholds: GresBalancerThresholds::default(),
                registry_layout: GresBalancerRegistryLayout::default(),
                plan_snapshot: None,
            }),
        },
    );
    obj.metadata.namespace = Some("ns".into());
    obj.metadata.uid = Some("gres-uid".into());
    obj.metadata.generation = Some(3);
    obj
}

fn tenant_list_body() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "GresTenantList",
        "items": [{
            "apiVersion": "crabka.io/v1alpha1",
            "kind": "GresTenant",
            "metadata": { "name": "tenant-a", "namespace": "ns", "uid": "tenant-uid" },
            "spec": { "gres": "fleet", "user": "alice", "passwordSecretRef": { "name": "pw", "key": "password" }, "suspended": false }
        }]
    })
}

fn tenant_list_body_with_multi_range_tenant() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "GresTenantList",
        "items": [
            {
                "apiVersion": "crabka.io/v1alpha1",
                "kind": "GresTenant",
                "metadata": { "name": "tenant-a", "namespace": "ns", "uid": "tenant-a-uid" },
                "spec": { "gres": "fleet", "user": "alice", "passwordSecretRef": { "name": "pw", "key": "password" }, "suspended": false }
            },
            {
                "apiVersion": "crabka.io/v1alpha1",
                "kind": "GresTenant",
                "metadata": { "name": "tenant-b", "namespace": "ns", "uid": "tenant-b-uid" },
                "spec": {
                    "gres": "fleet", "user": "bob", "passwordSecretRef": { "name": "pw", "key": "password" },
                    "ranges": [
                        { "rangeId": 0, "endKey": { "tableId": 100, "rowid": 0 } },
                        { "rangeId": 1 }
                    ]
                }
            }
        ]
    })
}

fn admin_secret_body() -> serde_json::Value {
    use base64::Engine as _;

    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "admin", "namespace": "ns", "uid": "admin-uid" },
        "type": "Opaque",
        "data": {
            "password": base64::engine::general_purpose::STANDARD.encode(b"pw")
        }
    })
}

fn gres_tenant(name: &str, gres_name: &str) -> GresTenant {
    let mut tenant = GresTenant::new(
        name,
        GresTenantSpec {
            gres: gres_name.into(),
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
    tenant
}

fn reconcile_rules(include_status: bool) -> Vec<MockRule> {
    reconcile_rules_with_registry(include_status, None)
}

fn reconcile_rules_with_registry(
    include_status: bool,
    gres_registry: Option<serde_json::Value>,
) -> Vec<MockRule> {
    let mut kafka = serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": "demo", "namespace": "ns" },
        "spec": { "kafkaVersion": "0.1.1" }
    });
    if let Some(policy) = gres_registry {
        kafka["spec"]["gresRegistry"] = policy;
    }
    let mut rules = vec![
        MockRule {
            method: Method::GET,
            path_substr: "/kafkas/demo".into(),
            response: json_response(200, &kafka),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/grestenants".into(),
            response: json_response(200, &tenant_list_body()),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/secrets/fleet-pgdog-config".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"v1","kind":"Secret","metadata":{"name":"fleet-pgdog-config","namespace":"ns"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/fleet-gres-activator".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"fleet-gres-activator","namespace":"ns"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/services/fleet-pgdog".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"fleet-pgdog","namespace":"ns"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/fleet-gres-activator".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fleet-gres-activator","namespace":"ns"}}),
            ),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: "/deployments/fleet-pgdog".into(),
            response: json_response(
                200,
                &serde_json::json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fleet-pgdog","namespace":"ns"}}),
            ),
        },
        MockRule {
            method: Method::GET,
            path_substr: "/secrets/admin".into(),
            response: json_response(200, &admin_secret_body()),
        },
    ];
    if include_status {
        rules.push(MockRule {
            method: Method::PATCH,
            path_substr: "/greses/fleet/status".into(),
            response: json_response(200, &serde_json::to_value(gres()).unwrap()),
        });
    }
    rules
}

#[tokio::test]
async fn renders_pgdog_config_secret_and_status_hash() {
    let admin = FakePgdogAdmin::new(vec![false, true]);
    let rules = reconcile_rules(true);
    let state = MockState::new(rules);
    let ctx = Arc::new(
        fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin.clone()),
    );

    reconcile(Arc::new(gres()), ctx).await.unwrap();

    let observed = state.take_observed();
    let secret_patch = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/secrets/fleet-pgdog-config")
        })
        .expect("config Secret patch captured");
    let secret_body: serde_json::Value = serde_json::from_slice(secret_patch.body()).unwrap();
    assert!(secret_body["data"]["pgdog.toml"].is_string());
    assert!(secret_body["data"]["users.toml"].is_string());
    let pgdog_toml = {
        use base64::Engine as _;
        let encoded = secret_body["data"]["pgdog.toml"]
            .as_str()
            .expect("pgdog.toml base64");
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .expect("decode pgdog.toml"),
        )
        .expect("pgdog.toml UTF-8")
    };
    for expected in [
        "pooler_mode = \"transaction\"",
        "connect_timeout = 30000",
        "connect_attempts = 3",
        "checkout_timeout = 90000",
        "idle_timeout = 60000",
        "server_lifetime = 300000",
    ] {
        assert!(
            pgdog_toml.contains(expected),
            "missing {expected}: {pgdog_toml}"
        );
    }
    assert!(
        pgdog_toml.contains(
            "[[databases]]\nname = \"tenant-a\"\nhost = \"tenant-a-gres.ns.svc.cluster.local\"\nport = 5432\npooler_mode = \"transaction\""
        ),
        "got: {pgdog_toml}"
    );
    let users_toml = {
        use base64::Engine as _;
        let encoded = secret_body["data"]["users.toml"]
            .as_str()
            .expect("users.toml base64");
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .expect("decode users.toml"),
        )
        .expect("users.toml UTF-8")
    };
    assert!(users_toml.contains("name = \"alice\""));
    assert!(users_toml.contains("database = \"tenant-a\""));
    assert!(!users_toml.contains("password"));

    let deployment_patch = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/deployments/fleet-pgdog")
        })
        .expect("deployment patch captured");
    let deployment_body: serde_json::Value =
        serde_json::from_slice(deployment_patch.body()).unwrap();
    assert!(
        deployment_body["spec"]["template"]["spec"]["containers"][0]["image"]
            == "ghcr.io/pgdogdev/pgdog:0.1.47"
    );
    assert!(
        deployment_body["spec"]["template"]["spec"]["containers"][0]["env"][0]
            == serde_json::json!({
                "name": "PGDOG_ADMIN_PASSWORD",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "admin",
                        "key": "password"
                    }
                }
            })
    );
    assert!(
        deployment_body["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["periodSeconds"]
            == 5
    );
    assert!(observed.iter().any(|request| {
        request
            .uri()
            .to_string()
            .contains("/services/fleet-gres-activator")
    }));
    let activator_deployment = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/deployments/fleet-gres-activator")
        })
        .expect("activator deployment patch captured");
    let activator_body: serde_json::Value =
        serde_json::from_slice(activator_deployment.body()).unwrap();
    assert!(
        activator_body["spec"]["template"]["spec"]["containers"][0]["image"]
            .as_str()
            .is_some_and(|image| image.starts_with("ghcr.io/robot-head/crabka-gres-activator:"))
    );
    assert!(activator_body["spec"]["replicas"] == 1);
    assert!(
        activator_body["spec"]["template"]["spec"]["containers"][0]["args"]
            == serde_json::json!([
                "--listen",
                "0.0.0.0:6543",
                "--bootstrap",
                "demo-plain-bootstrap.ns.svc:9092",
                "--registry-poll-ms",
                "250",
                "--cold-start-timeout-ms",
                "30000",
                "--registry-replication-factor",
                "1",
                "--registry-topic-create-timeout-ms",
                "15000",
                "--registry-reader-retry-backoff-ms",
                "250",
                "--registry-fetch-max-wait-ms",
                "500",
                "--registry-fetch-partition-max-bytes",
                "1048576",
                "--backend-endpoint-template",
                "{tenant}-gres.ns.svc:5432"
            ])
    );
    assert!(
        activator_body["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["periodSeconds"]
            == 5
    );

    let status_patch = observed
        .iter()
        .find(|request| request.uri().to_string().contains("/greses/fleet/status"))
        .expect("status patch captured");
    let status_body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    assert!(
        status_body["status"]["confirmedPgdogConfigHash"]
            .as_str()
            .unwrap()
            .len()
            == 64
    );
    let admin_requests = admin.requests();
    assert!(admin_requests.len() == 2);
    assert!(admin_requests[0][0].expected_routes[0].database == "tenant-a");
    assert!(admin_requests[0][0].expected_routes[0].host == "tenant-a-gres.ns.svc.cluster.local");
    assert!(!admin_requests[0][0].maintenance_mode);
    assert!(admin_requests[0][0].tls_ca_pem.is_none());
    assert!(admin_requests[0][0].connect_addr.is_none());
    assert!(status_body["status"]["balancer"]["dryRunOnly"] == true);
    assert!(status_body["status"]["balancer"]["plannedOperations"] == 0);
    assert!(status_body["status"]["balancer"]["executableOperations"] == 0);
    assert!(status_body["status"]["balancer"]["unsupportedOperations"] == 0);
    assert!(status_body["status"]["balancer"]["disabledGoals"] == serde_json::json!(["load_skew"]));
    assert!(
        status_body["status"]["balancer"]["enabledGoals"]
            == serde_json::json!([
                "co_location_integrity",
                "range_limit",
                "range_size",
                "auto_shard_conversion"
            ])
    );
    assert!(status_body["status"]["balancer"]["plannedOperationKinds"] == serde_json::json!([]));
    assert!(
        status_body["status"]["balancer"]["mutationDisabledReason"]
            .as_str()
            .is_some_and(|reason| reason.contains("protocol is not configured"))
    );
    assert!(
        status_body["status"]["balancer"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("no dry-run planner snapshot"))
    );
}

#[tokio::test]
async fn custom_activator_policy_renders_workload_and_pgdog_timeout_budget() {
    let admin = FakePgdogAdmin::new(vec![true]);
    let state = MockState::new(reconcile_rules_with_registry(
        true,
        Some(serde_json::json!({
            "replicationFactor": 32767,
            "topicCreateTimeoutMs": 15001,
            "readerRetryBackoffMs": 251,
            "fetchMaxWaitMs": 501,
            "fetchPartitionMaxBytes": 1_048_577
        })),
    ));
    let mut context = fixture_ctx(mock_client(&state, "ns"), "ns");
    Arc::get_mut(&mut context.config)
        .expect("fixture config is uniquely owned")
        .default_gres_activator_image = Some("example.test/global-activator:v1".into());
    let ctx = Arc::new(context.with_pgdog_admin_for_test(admin));
    let mut obj = gres();
    obj.spec.activator = Some(GresActivatorSpec {
        image: Some("example.test/activator:v2".into()),
        replicas: Some(4),
        registry_poll_ms: Some(600),
        cold_start_timeout_ms: Some(40_000),
        readiness_probe_period_seconds: Some(9),
    });
    obj.spec.pgdog.pooler_mode = Some(PgdogPoolerModeSpec::Session);
    obj.spec.pgdog.connect_attempts = Some(4);
    obj.spec.pgdog.idle_timeout_ms = Some(61_000);
    obj.spec.pgdog.suspension_idle_timeout_ms = Some(1_500);
    obj.spec.pgdog.server_lifetime_ms = Some(301_000);
    obj.spec.pgdog.readiness_probe_period_seconds = Some(6);

    reconcile(Arc::new(obj), ctx).await.unwrap();

    let observed = state.take_observed();
    let activator_patch = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/deployments/fleet-gres-activator")
        })
        .expect("activator deployment patch captured");
    let activator: serde_json::Value =
        serde_json::from_slice(activator_patch.body()).expect("activator deployment JSON");
    assert!(
        activator["spec"]["template"]["spec"]["containers"][0]["image"]
            == "example.test/activator:v2"
    );
    assert!(activator["spec"]["replicas"] == 4);
    assert!(
        activator["spec"]["template"]["spec"]["containers"][0]["args"]
            == serde_json::json!([
                "--listen",
                "0.0.0.0:6543",
                "--bootstrap",
                "demo-plain-bootstrap.ns.svc:9092",
                "--registry-poll-ms",
                "600",
                "--cold-start-timeout-ms",
                "40000",
                "--registry-replication-factor",
                "32767",
                "--registry-topic-create-timeout-ms",
                "15001",
                "--registry-reader-retry-backoff-ms",
                "251",
                "--registry-fetch-max-wait-ms",
                "501",
                "--registry-fetch-partition-max-bytes",
                "1048577",
                "--backend-endpoint-template",
                "{tenant}-gres.ns.svc:5432"
            ])
    );
    assert!(
        activator["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["periodSeconds"]
            == 9
    );

    let secret_patch = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/secrets/fleet-pgdog-config")
        })
        .expect("config Secret patch captured");
    let secret: serde_json::Value =
        serde_json::from_slice(secret_patch.body()).expect("config Secret JSON");
    let pgdog_toml = {
        use base64::Engine as _;
        let encoded = secret["data"]["pgdog.toml"]
            .as_str()
            .expect("pgdog.toml base64");
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .expect("decode pgdog.toml"),
        )
        .expect("pgdog.toml UTF-8")
    };
    assert!(pgdog_toml.contains("pooler_mode = \"session\""));
    assert!(pgdog_toml.contains("connect_timeout = 40000"));
    assert!(pgdog_toml.contains("connect_attempts = 4"));
    assert!(pgdog_toml.contains("checkout_timeout = 160000"));
    assert!(pgdog_toml.contains("idle_timeout = 61000"));
    assert!(pgdog_toml.contains("server_lifetime = 301000"));
    assert!(
        pgdog_toml.contains(
            "[[databases]]\nname = \"tenant-a\"\nhost = \"tenant-a-gres.ns.svc.cluster.local\"\nport = 5432\npooler_mode = \"session\""
        ),
        "got: {pgdog_toml}"
    );
    let pgdog_deployment = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/deployments/fleet-pgdog")
        })
        .expect("PgDog deployment patch captured");
    let pgdog_deployment: serde_json::Value =
        serde_json::from_slice(pgdog_deployment.body()).expect("PgDog deployment JSON");
    assert!(
        pgdog_deployment["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["periodSeconds"]
            == 6
    );
}

#[tokio::test]
async fn matching_effective_idle_policy_selects_suspension_timeout() {
    let admin = FakePgdogAdmin::new(vec![true]);
    let mut rules = reconcile_rules(true);
    rules
        .iter_mut()
        .find(|rule| rule.path_substr == "/grestenants")
        .expect("tenant list rule")
        .response = json_response(
        200,
        &serde_json::json!({
            "apiVersion": "crabka.io/v1alpha1",
            "kind": "GresTenantList",
            "items": [
                {
                    "apiVersion": "crabka.io/v1alpha1",
                    "kind": "GresTenant",
                    "metadata": { "name": "tenant-a", "namespace": "ns" },
                    "spec": {
                        "gres": "fleet",
                        "user": "alice",
                        "passwordSecretRef": { "name": "pw", "key": "password" },
                        "overrides": { "idleSeconds": 2 }
                    }
                },
                {
                    "apiVersion": "crabka.io/v1alpha1",
                    "kind": "GresTenant",
                    "metadata": { "name": "tenant-b", "namespace": "ns" },
                    "spec": {
                        "gres": "other",
                        "user": "bob",
                        "passwordSecretRef": { "name": "pw", "key": "password" },
                        "overrides": { "idleSeconds": 9 }
                    }
                }
            ]
        }),
    );
    let state = MockState::new(rules);
    let ctx =
        Arc::new(fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin));
    let mut obj = gres();
    obj.spec.pgdog.idle_timeout_ms = Some(61_000);
    obj.spec.pgdog.suspension_idle_timeout_ms = Some(1_500);

    reconcile(Arc::new(obj), ctx).await.unwrap();

    let observed = state.take_observed();
    let secret = observed
        .iter()
        .find(|request| request.uri().path().contains("/secrets/fleet-pgdog-config"))
        .expect("config Secret patch");
    let body: serde_json::Value = serde_json::from_slice(secret.body()).unwrap();
    let pgdog_toml = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        body["data"]["pgdog.toml"].as_str().unwrap(),
    )
    .unwrap();
    let pgdog_toml = String::from_utf8(pgdog_toml).unwrap();
    assert!(
        pgdog_toml.contains("idle_timeout = 1500"),
        "got: {pgdog_toml}"
    );
}

#[tokio::test]
async fn zero_override_and_unrelated_fleet_do_not_select_suspension_timeout() {
    let admin = FakePgdogAdmin::new(vec![true]);
    let mut rules = reconcile_rules(true);
    rules
        .iter_mut()
        .find(|rule| rule.path_substr == "/grestenants")
        .expect("tenant list rule")
        .response = json_response(
        200,
        &serde_json::json!({
            "apiVersion": "crabka.io/v1alpha1",
            "kind": "GresTenantList",
            "items": [
                {
                    "apiVersion": "crabka.io/v1alpha1",
                    "kind": "GresTenant",
                    "metadata": { "name": "tenant-a", "namespace": "ns" },
                    "spec": {
                        "gres": "fleet",
                        "user": "alice",
                        "passwordSecretRef": { "name": "pw", "key": "password" },
                        "overrides": { "idleSeconds": 0 }
                    }
                },
                {
                    "apiVersion": "crabka.io/v1alpha1",
                    "kind": "GresTenant",
                    "metadata": { "name": "tenant-b", "namespace": "ns" },
                    "spec": {
                        "gres": "other",
                        "user": "bob",
                        "passwordSecretRef": { "name": "pw", "key": "password" },
                        "overrides": { "idleSeconds": 9 }
                    }
                }
            ]
        }),
    );
    let state = MockState::new(rules);
    let ctx =
        Arc::new(fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin));
    let mut obj = gres();
    obj.spec.defaults = Some(TenantDefaults {
        idle_seconds: Some(8),
        wal_replication: None,
        checkpoint_frames: None,
        checkpoint_bytes: None,
        suspend_max_checkpoint_bytes: None,
    });
    obj.spec.pgdog.idle_timeout_ms = Some(61_000);
    obj.spec.pgdog.suspension_idle_timeout_ms = Some(1_500);

    reconcile(Arc::new(obj), ctx).await.unwrap();

    let observed = state.take_observed();
    let secret = observed
        .iter()
        .find(|request| request.uri().path().contains("/secrets/fleet-pgdog-config"))
        .expect("config Secret patch");
    let body: serde_json::Value = serde_json::from_slice(secret.body()).unwrap();
    let pgdog_toml = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        body["data"]["pgdog.toml"].as_str().unwrap(),
    )
    .unwrap();
    let pgdog_toml = String::from_utf8(pgdog_toml).unwrap();
    assert!(
        pgdog_toml.contains("idle_timeout = 61000"),
        "got: {pgdog_toml}"
    );
}

#[tokio::test]
async fn invalid_pgdog_policy_fails_before_kubernetes_io() {
    let cases = [
        (
            "spec.pgdog.listenPort",
            -1,
            Some(3),
            Some(60_000),
            Some(1_000),
            Some(300_000),
            Some(5),
            Some(4_000),
            30_000,
        ),
        (
            "spec.pgdog.connectAttempts",
            6_432,
            Some(0),
            Some(60_000),
            Some(1_000),
            Some(300_000),
            Some(5),
            Some(4_000),
            30_000,
        ),
        (
            "spec.pgdog.idleTimeoutMs",
            6_432,
            Some(3),
            Some(0),
            Some(1_000),
            Some(300_000),
            Some(5),
            Some(4_000),
            30_000,
        ),
        (
            "spec.pgdog.suspensionIdleTimeoutMs",
            6_432,
            Some(3),
            Some(60_000),
            Some(0),
            Some(300_000),
            Some(5),
            Some(4_000),
            30_000,
        ),
        (
            "spec.pgdog.serverLifetimeMs",
            6_432,
            Some(3),
            Some(60_000),
            Some(1_000),
            Some(0),
            Some(5),
            Some(4_000),
            30_000,
        ),
        (
            "spec.pgdog.readinessProbePeriodSeconds",
            6_432,
            Some(3),
            Some(60_000),
            Some(1_000),
            Some(300_000),
            Some(0),
            Some(4_000),
            30_000,
        ),
        (
            "spec.pgdog.directBootstrapGraceMs",
            6_432,
            Some(3),
            Some(60_000),
            Some(1_000),
            Some(300_000),
            Some(5),
            Some(0),
            30_000,
        ),
        (
            "spec.activator.coldStartTimeoutMs",
            6_432,
            Some(65_535),
            Some(60_000),
            Some(1_000),
            Some(300_000),
            Some(5),
            Some(4_000),
            u64::MAX,
        ),
    ];

    for (
        path,
        listen_port,
        attempts,
        idle,
        suspension_idle,
        lifetime,
        readiness,
        grace,
        attempt_timeout,
    ) in cases
    {
        let state = MockState::new(Vec::new());
        let ctx = Arc::new(fixture_ctx(mock_client(&state, "ns"), "ns"));
        let mut obj = gres();
        obj.spec.pgdog.listen_port = listen_port;
        obj.spec.pgdog.connect_attempts = attempts;
        obj.spec.pgdog.idle_timeout_ms = idle;
        obj.spec.pgdog.suspension_idle_timeout_ms = suspension_idle;
        obj.spec.pgdog.server_lifetime_ms = lifetime;
        obj.spec.pgdog.readiness_probe_period_seconds = readiness;
        obj.spec.pgdog.direct_bootstrap_grace_ms = grace;
        obj.spec.activator = Some(GresActivatorSpec {
            cold_start_timeout_ms: Some(attempt_timeout),
            ..Default::default()
        });

        let error = reconcile(Arc::new(obj), ctx)
            .await
            .expect_err("invalid PgDog policy must fail");

        assert!(error.to_string().contains(path), "got: {error}");
        assert!(
            state.take_observed().is_empty(),
            "Kubernetes I/O for {path}"
        );
    }
}

#[tokio::test]
async fn global_activator_image_is_used_when_crd_omits_image() {
    let admin = FakePgdogAdmin::new(vec![true]);
    let state = MockState::new(reconcile_rules(true));
    let mut context = fixture_ctx(mock_client(&state, "ns"), "ns");
    Arc::get_mut(&mut context.config)
        .expect("fixture config is uniquely owned")
        .default_gres_activator_image = Some("example.test/global-activator:v1".into());
    let ctx = Arc::new(context.with_pgdog_admin_for_test(admin));

    reconcile(Arc::new(gres()), ctx).await.unwrap();

    let observed = state.take_observed();
    let activator_patch = observed
        .iter()
        .find(|request| {
            request
                .uri()
                .to_string()
                .contains("/deployments/fleet-gres-activator")
        })
        .expect("activator deployment patch captured");
    let activator: serde_json::Value =
        serde_json::from_slice(activator_patch.body()).expect("activator deployment JSON");

    assert!(
        activator["spec"]["template"]["spec"]["containers"][0]["image"]
            == "example.test/global-activator:v1"
    );
}

#[tokio::test]
async fn invalid_activator_values_fail_before_kubernetes_io() {
    let cases = [
        (
            "spec.activator.image",
            GresActivatorSpec {
                image: Some(String::new()),
                ..Default::default()
            },
        ),
        (
            "spec.activator.replicas",
            GresActivatorSpec {
                replicas: Some(0),
                ..Default::default()
            },
        ),
        (
            "spec.activator.registryPollMs",
            GresActivatorSpec {
                registry_poll_ms: Some(0),
                ..Default::default()
            },
        ),
        (
            "spec.activator.coldStartTimeoutMs",
            GresActivatorSpec {
                cold_start_timeout_ms: Some(0),
                ..Default::default()
            },
        ),
        (
            "spec.activator.readinessProbePeriodSeconds",
            GresActivatorSpec {
                readiness_probe_period_seconds: Some(0),
                ..Default::default()
            },
        ),
    ];

    for (path, activator) in cases {
        let state = MockState::new(Vec::new());
        let ctx = Arc::new(fixture_ctx(mock_client(&state, "ns"), "ns"));
        let mut obj = gres();
        obj.spec.activator = Some(activator);

        let error = reconcile(Arc::new(obj), ctx)
            .await
            .expect_err("invalid activator policy must fail");

        assert!(error.to_string().contains(path), "got: {error}");
        assert!(
            state.take_observed().is_empty(),
            "Kubernetes I/O for {path}"
        );
    }
}

#[tokio::test]
async fn missing_kafka_causes_no_child_writes() {
    let state = MockState::new(Vec::new());
    let ctx = Arc::new(fixture_ctx(mock_client(&state, "ns"), "ns"));

    let error = reconcile(Arc::new(gres()), ctx)
        .await
        .expect_err("missing Kafka must fail");

    assert!(
        error
            .to_string()
            .contains("referenced Kafka demo does not exist")
    );
    let observed = state.take_observed();
    assert!(observed.len() == 1);
    assert!(observed[0].method() == Method::GET);
    assert!(observed[0].uri().to_string().contains("/kafkas/demo"));
}

#[tokio::test]
async fn invalid_kafka_registry_policy_causes_no_child_writes() {
    let kafka = serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": "demo", "namespace": "ns" },
        "spec": {
            "kafkaVersion": "0.1.1",
            "gresRegistry": { "replicationFactor": 32768 }
        }
    });
    let state = MockState::new(vec![MockRule {
        method: Method::GET,
        path_substr: "/kafkas/demo".into(),
        response: json_response(200, &kafka),
    }]);
    let ctx = Arc::new(fixture_ctx(mock_client(&state, "ns"), "ns"));

    let error = reconcile(Arc::new(gres()), ctx)
        .await
        .expect_err("invalid Kafka registry policy must fail");

    assert!(error.to_string().contains("spec.gresRegistry"));
    let observed = state.take_observed();
    assert!(observed.len() == 1);
    assert!(observed[0].method() == Method::GET);
}

#[tokio::test]
async fn multi_replica_reload_requests_maintenance_mode() {
    let admin = FakePgdogAdmin::new(vec![true]);
    let mut rules = reconcile_rules(true);
    rules.push(MockRule {
        method: Method::GET,
        path_substr: "/namespaces/ns/pods".into(),
        response: json_response(
            200,
            &serde_json::json!({
                "apiVersion": "v1", "kind": "PodList", "items": [
                    {"metadata":{"name":"pgdog-0"},"status":{"podIP":"10.0.0.10"}},
                    {"metadata":{"name":"pgdog-1"},"status":{"podIP":"10.0.0.11"}}
                ]
            }),
        ),
    });
    rules.push(MockRule {
        method: Method::GET,
        path_substr: "/secrets/pgdog-tls".into(),
        response: json_response(200, &serde_json::json!({
            "apiVersion":"v1", "kind":"Secret", "metadata":{"name":"pgdog-tls","namespace":"ns"},
            "data":{
                "ca.crt":"dGVzdC1jYQ==",
                "tls.crt":"dGVzdC1jZXJ0",
                "tls.key":"dGVzdC1rZXk=",
                "client.crt":"dGVzdC1jbGllbnQtY2VydA==",
                "client.key":"dGVzdC1jbGllbnQta2V5"
            }
        })),
    });
    let state = MockState::new(rules);
    let ctx = Arc::new(
        fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin.clone()),
    );
    let mut obj = gres();
    obj.spec.pgdog.replicas = 2;
    obj.spec.pgdog.tls_secret_ref = Some(SecretRef {
        name: "pgdog-tls".into(),
    });

    reconcile(Arc::new(obj), ctx).await.unwrap();

    let requests = admin.requests();
    let expected_requests = ["10.0.0.10", "10.0.0.11"]
        .into_iter()
        .map(|address| PgdogReloadRequest {
            host: "fleet-pgdog.ns.svc.cluster.local".into(),
            connect_addr: Some(address.parse::<IpAddr>().expect("fixture IP address")),
            port: 6432,
            password: "pw".into(),
            expected_routes: vec![PgdogExpectedRoute {
                database: "tenant-a".into(),
                host: "tenant-a-gres.ns.svc.cluster.local".into(),
                port: 5432,
            }],
            maintenance_mode: true,
            tls_ca_pem: Some(b"test-ca".to_vec()),
            tls_client_identity_pem: Some((
                b"test-client-cert".to_vec(),
                b"test-client-key".to_vec(),
            )),
        })
        .collect::<Vec<_>>();
    assert!(requests == vec![expected_requests]);
}

#[tokio::test]
async fn reconciled_balancer_plan_without_protocol_keeps_all_operations_disabled() {
    let admin = FakePgdogAdmin::new(vec![true]);
    let state = MockState::new(reconcile_rules(true));
    let ctx =
        Arc::new(fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin));
    let mut obj = gres();
    obj.spec
        .balancer
        .as_mut()
        .expect("fixture configures the balancer")
        .plan_snapshot = Some(GresBalancerPlanSnapshot {
        operations: vec![
            GresBalancerOperationKind::Split,
            GresBalancerOperationKind::Merge,
            GresBalancerOperationKind::Move,
            GresBalancerOperationKind::ConvertToSharded,
        ],
    });

    reconcile(Arc::new(obj), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .find(|request| request.uri().to_string().contains("/greses/fleet/status"))
        .expect("status patch captured");
    let status_body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let balancer = &status_body["status"]["balancer"];
    assert!(balancer["plannedOperations"] == 4);
    assert!(
        balancer["plannedOperationKinds"]
            == serde_json::json!(["split", "merge", "move", "convert_to_sharded"])
    );
    assert!(balancer["executableOperations"] == 0);
    assert!(balancer["executableOperationKinds"] == serde_json::json!([]));
    assert!(balancer["unsupportedOperations"] == 4);
    assert!(balancer["unsupportedOperationKinds"] == balancer["plannedOperationKinds"]);
    assert!(
        balancer["message"]
            .as_str()
            .is_some_and(|message| message.contains("mutations remain disabled"))
    );
}

#[tokio::test]
async fn reconciled_balancer_plan_with_protocol_reports_physical_operations_unavailable() {
    let admin = FakePgdogAdmin::new(vec![true]);
    let state = MockState::new(reconcile_rules(true));
    let ctx =
        Arc::new(fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin));
    let mut obj = gres();
    let balancer_spec = obj
        .spec
        .balancer
        .as_mut()
        .expect("fixture configures the balancer");
    balancer_spec
        .registry_layout
        .transactional_registry_protocol = true;
    balancer_spec.plan_snapshot = Some(GresBalancerPlanSnapshot {
        operations: vec![
            GresBalancerOperationKind::Move,
            GresBalancerOperationKind::ConvertToSharded,
            GresBalancerOperationKind::Split,
            GresBalancerOperationKind::Merge,
            GresBalancerOperationKind::Move,
        ],
    });

    reconcile(Arc::new(obj), ctx).await.unwrap();

    let observed = state.take_observed();
    let status_patch = observed
        .iter()
        .find(|request| request.uri().to_string().contains("/greses/fleet/status"))
        .expect("status patch captured");
    let status_body: serde_json::Value = serde_json::from_slice(status_patch.body()).unwrap();
    let balancer = &status_body["status"]["balancer"];
    assert!(balancer["transactionalRegistryProtocolAvailable"] == true);
    assert!(balancer["plannedOperations"] == 5);
    assert!(
        balancer["plannedOperationKinds"]
            == serde_json::json!(["move", "convert_to_sharded", "split", "merge"])
    );
    assert!(balancer["executableOperations"] == 0);
    assert!(balancer["executableOperationKinds"] == serde_json::json!([]));
    assert!(balancer["unsupportedOperations"] == 5);
    assert!(
        balancer["unsupportedOperationKinds"]
            == serde_json::json!(["move", "convert_to_sharded", "split", "merge"])
    );
    assert!(
        balancer["mutationDisabledReason"]
            .as_str()
            .is_some_and(|reason| reason.contains("checkpoint, copy, catch-up, and cutover"))
    );
}

#[tokio::test]
async fn stale_pgdog_admin_view_requeues_without_confirming_hash() {
    let admin = FakePgdogAdmin::new(vec![false, false]);
    let state = MockState::new(reconcile_rules(false));
    let mut context =
        fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin.clone());
    let config = Arc::get_mut(&mut context.config).expect("unique config");
    config.pgdog_reload_attempts = "2".parse().expect("positive attempts");
    config.pgdog_reload_backoff_ms = "150".parse().expect("positive backoff");
    config.pgdog_reload_requeue_ms = "1234".parse().expect("positive requeue");
    let ctx = Arc::new(context);

    let action = reconcile(Arc::new(gres()), ctx).await.unwrap();

    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(1_234))
    );
    assert!(admin.requests().len() == 2);
    let call_times = admin.call_times();
    let retry_delay = call_times[1].duration_since(call_times[0]);
    assert!(retry_delay >= Duration::from_millis(150));
    assert!(retry_delay < Duration::from_secs(1));
    assert!(state.remaining_rules() == 0);
    let observed = state.take_observed();
    assert!(
        !observed
            .iter()
            .any(|request| request.uri().to_string().contains("/greses/fleet/status"))
    );
}

#[tokio::test]
async fn gres_error_policy_uses_configured_requeue() {
    let state = MockState::new(Vec::new());
    let mut context = fixture_ctx(mock_client(&state, "ns"), "ns");
    Arc::get_mut(&mut context.config)
        .expect("unique config")
        .controller_error_requeue_ms = "4321".parse().expect("positive error requeue");

    let action = error_policy(
        Arc::new(gres()),
        &ReconcileError::Malformed("test".into()),
        Arc::new(context),
    );

    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(4_321))
    );
}

#[tokio::test]
async fn pgdog_admin_reload_uses_configured_timeout() {
    let admin = FakePgdogAdmin::with_delay(vec![true], Duration::from_millis(20));
    let state = MockState::new(reconcile_rules(false));
    let mut context = fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin);
    Arc::get_mut(&mut context.config)
        .expect("unique config")
        .pgdog_admin_timeout_ms = "1".parse().expect("positive timeout");

    let error = reconcile(Arc::new(gres()), Arc::new(context))
        .await
        .expect_err("admin reload should time out");

    assert!(
        matches!(
            error,
            ReconcileError::PgdogAdmin(PgdogAdminError::Fleet(ref message))
                if message.contains("1ms")
        ),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn fleet_reconcile_excludes_unsupported_multi_range_tenants_from_pgdog_config_and_reload_expectations()
 {
    let admin = FakePgdogAdmin::new(vec![true]);
    let mut rules = reconcile_rules(true);
    rules
        .iter_mut()
        .find(|rule| rule.path_substr == "/grestenants")
        .expect("tenant list rule")
        .response = json_response(200, &tenant_list_body_with_multi_range_tenant());
    let state = MockState::new(rules);
    let ctx = Arc::new(
        fixture_ctx(mock_client(&state, "ns"), "ns").with_pgdog_admin_for_test(admin.clone()),
    );

    reconcile(Arc::new(gres()), ctx).await.unwrap();

    assert!(admin.requests()[0][0].expected_routes[0].database == "tenant-a");
    let observed = state.take_observed();
    let secret_patch = observed
        .iter()
        .find(|request| request.uri().path().contains("/secrets/fleet-pgdog-config"))
        .expect("config Secret patch captured");
    let body: serde_json::Value = serde_json::from_slice(secret_patch.body()).unwrap();
    let pgdog_toml = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        body["data"]["pgdog.toml"]
            .as_str()
            .expect("pgdog config data"),
    )
    .expect("base64 pgdog config");
    let pgdog_toml = String::from_utf8(pgdog_toml).expect("UTF-8 pgdog config");
    assert!(pgdog_toml.contains("tenant-a"));
    assert!(!pgdog_toml.contains("tenant-b"));
}

#[test]
fn tenant_watch_maps_to_referenced_gres_fleet() {
    let refs = tenant_to_gres_refs(&gres_tenant("tenant-a", "fleet"));

    assert!(refs.len() == 1);
    assert!(refs[0].name == "fleet");
    assert!(refs[0].namespace.as_deref() == Some("ns"));
}

#[test]
fn tenant_endpoint_uses_status_lifecycle_phase_for_activator_routing() {
    let mut tenant = gres_tenant("tenant-a", "fleet");
    tenant.status = Some(crabka_operator::crd::GresTenantStatus {
        lifecycle_phase: Some("resume_requested".into()),
        ..Default::default()
    });

    let endpoint = tenant_endpoint(&tenant).expect("valid endpoint");

    assert!(endpoint.state == crabka_gres_control::TenantState::ResumeRequested);
}

#[test]
fn multi_range_unsupported_tenant_is_excluded_from_pgdog_endpoint_set() {
    let mut tenant = gres_tenant("tenant-a", "fleet");
    tenant.status = Some(crabka_operator::crd::GresTenantStatus {
        conditions: vec![crabka_operator::crd::KafkaCondition {
            type_: "Ready".into(),
            status: "False".into(),
            reason: "MultiRangeUnsupported".into(),
            message: "multi-range placement is unavailable".into(),
            last_transition_time: "2026-07-10T00:00:00Z".into(),
        }],
        ..Default::default()
    });

    let endpoints: Vec<_> = [tenant].iter().filter_map(tenant_endpoint).collect();

    assert!(endpoints.is_empty());
}

#[test]
fn disabled_balancer_knob_reports_no_enabled_goals() {
    let mut obj = gres();
    obj.spec.balancer = Some(GresBalancerSpec {
        enabled: false,
        goals: GresBalancerGoals::default(),
        thresholds: GresBalancerThresholds::default(),
        registry_layout: GresBalancerRegistryLayout::default(),
        plan_snapshot: None,
    });

    let status = crabka_operator::controller::gres::balancer_status(&obj);

    assert!(!status.enabled);
    assert!(status.enabled_goals.is_empty());
    assert!(status.disabled_goals.len() == 5);
    assert!(status.planned_operations == 0);
    assert!(status.executable_operations == 0);
    assert!(status.unsupported_operations == 0);
}
