//! Reconcile-level tests for the `KafkaGrpcGateway` controller.
//!
//! FIFO rule order matches the exact call sequence in
//! `crates/operator/src/controller/grpc_gateway.rs::reconcile`:
//!
//!   1. GET kafkas/<parent>                    — fetch parent, version gate
//!   2. PATCH kafkausers/<gw>-broker           — SSA child `KafkaUser`
//!   3. GET secrets/<gw>-broker                — cert-issued gate
//!   4. GET secrets/<gw>-serving               — check existing serving cert
//!   5. GET secrets/<parent>-cluster-ca        — cluster-CA key (for signing)
//!   6. GET secrets/<parent>-cluster-ca-cert   — cluster-CA cert (for signing)
//!   7. PATCH secrets/<gw>-serving             — issue new serving cert
//!   8. PATCH secrets/<gw>-config              — apply rendered config Secret
//!   9. PATCH deployments/<gw>                 — apply Deployment
//!  10. PATCH services/<gw>                    — apply Service
//!  11. GET deployments/<gw>                   — read back ready-replica count
//!  12. PATCH kafkagrpcgateways/<gw>/status    — write final status

use std::{collections::BTreeMap, sync::Arc};

use assert2::{assert, check};
use crabka_operator::{
    controller::grpc_gateway::reconcile,
    crd::grpc_gateway::{KafkaGrpcGateway, KafkaGrpcGatewaySpec},
};
use http::{Method, Response};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, build_ctx, fake_broker_user_secret, fake_cluster_ca_cert_secret,
    fake_cluster_ca_key_secret, fake_config_secret, fake_deployment_body, fake_gateway_body,
    fake_kafkauser_body, fake_parent_kafka_body, fake_service_body, fake_serving_secret,
    json_response, not_found_body,
};

const NS: &str = "default";
const KAFKA: &str = "demo";
const GW: &str = "my-gw";

/// Construct a minimal `KafkaGrpcGateway` CR with the `crabka.io/cluster`
/// label pointing at `KAFKA`.
fn gw_cr(name: &str) -> KafkaGrpcGateway {
    let mut gw = KafkaGrpcGateway::new(
        name,
        KafkaGrpcGatewaySpec {
            replicas: None,
            image: None,
            resources: None,
            dedup: None,
            membership_topic: None,
            tuning: None,
            schema_registry: None,
            health_checks: None,
            tls: None,
            authz: None,
            webhooks: vec![],
            outbound_subscriptions: vec![],
            allowed_targets: vec![],
            telemetry: None,
        },
    );
    gw.metadata.namespace = Some(NS.into());
    gw.metadata.uid = Some("gw-uid".into());
    gw.metadata.generation = Some(1);
    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/cluster".into(), KAFKA.into());
    gw.metadata.labels = Some(labels);
    gw
}

#[test]
fn runtime_crd_surface_round_trips() {
    let gw: KafkaGrpcGateway = serde_json::from_value(serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "KafkaGrpcGateway",
        "metadata": { "name": GW, "namespace": NS },
        "spec": {
            "membershipTopic": "members-custom",
            "tuning": {
                "internalTopicReplicationFactor": 2,
                "internalTopicAllowReplicationFallback": false,
                "internalTopicCreateTimeoutMs": 7001,
                "internalTopicSegmentMs": 22001,
                "internalTopicMinCleanableDirtyRatioBasisPoints": 250,
                "consumerPollTimeoutMs": 501,
                "ownershipWarmupEmptyPolls": 3,
                "readinessPollIntervalMs": 251,
                "produceMaxBodyBytes": 3_145_728,
                "forwardMaxBodyBytes": 3_145_727
            },
            "schemaRegistry": {
                "url": "http://registry:8081",
                "latestCacheTtlMs": 5001,
                "frameRaw": true
            },
            "healthChecks": {
                "readinessInitialDelaySeconds": 3,
                "readinessPeriodSeconds": 6,
                "livenessInitialDelaySeconds": 11,
                "livenessPeriodSeconds": 12
            },
            "dedup": { "ownershipGroup": "owners-custom" },
            "tls": { "reloadIntervalSecs": 31 },
            "authz": {
                "bearer": { "allowableClockSkewMs": 31001 }
            },
            "webhooks": [{
                "name": "orders",
                "targetTopic": "orders",
                "schemaSubject": "orders-value",
                "schemaFormat": "avro"
            }],
            "outboundSubscriptions": [{
                "name": "deliver",
                "sourceTopics": ["orders"],
                "targetUrl": "https://example.com/hook",
                "groupId": "deliver-custom",
                "decodeToJson": true
            }]
        }
    }))
    .expect("gateway CRD shape");

    let spec = serde_json::to_value(&gw.spec).expect("serialize spec");
    for pointer in [
        "/membershipTopic",
        "/tuning/internalTopicReplicationFactor",
        "/tuning/produceMaxBodyBytes",
        "/tuning/forwardMaxBodyBytes",
        "/schemaRegistry/latestCacheTtlMs",
        "/healthChecks/readinessPeriodSeconds",
        "/dedup/ownershipGroup",
        "/tls/reloadIntervalSecs",
        "/authz/bearer/allowableClockSkewMs",
        "/webhooks/0/schemaSubject",
        "/outboundSubscriptions/0/groupId",
        "/outboundSubscriptions/0/decodeToJson",
    ] {
        assert!(
            spec.pointer(pointer).is_some(),
            "missing {pointer} from {spec}"
        );
    }
}

#[tokio::test]
async fn runtime_invalid_values_stop_before_child_rendering() {
    for invalid in [
        serde_json::json!({"tuning": {"consumerPollTimeoutMs": 0}}),
        serde_json::json!({"tuning": {"produceMaxBodyBytes": 0}}),
        serde_json::json!({"tuning": {"forwardMaxBodyBytes": 0}}),
        serde_json::json!({
            "tuning": {"internalTopicMinCleanableDirtyRatioBasisPoints": 10001}
        }),
        serde_json::json!({"outboundSubscriptions": [{
            "name": "s",
            "sourceTopics": ["t"],
            "targetUrl": "https://example.com",
            "baseBackoffMs": 2,
            "maxBackoffMs": 1
        }]}),
        serde_json::json!({"schemaRegistry": {"url": "not a URL"}}),
        serde_json::json!({"healthChecks": {"readinessInitialDelaySeconds": -1}}),
        serde_json::json!({"healthChecks": {"livenessPeriodSeconds": 0}}),
        serde_json::json!({"webhooks": [{
            "name": "w",
            "targetTopic": "t",
            "secretRef": {"name": "secret", "key": "hmac"}
        }]}),
        serde_json::json!({"webhooks": [{
            "name": "w",
            "targetTopic": "t",
            "signatureHeader": "X-Signature"
        }]}),
        serde_json::json!({"webhooks": [{
            "name": "w",
            "targetTopic": "t",
            "idempotencySource": "cookie:id",
        }]}),
        serde_json::json!({"webhooks": [{
            "name": "w",
            "targetTopic": "t",
            "keySource": "json:$[",
        }]}),
        serde_json::json!({"outboundSubscriptions": [{
            "name": "s",
            "sourceTopics": ["t"],
            "targetUrl": "not a URL"
        }]}),
        serde_json::json!({"outboundSubscriptions": [{
            "name": "s",
            "sourceTopics": ["t"],
            "targetUrl": "mailto:user@example.com"
        }]}),
        serde_json::json!({"outboundSubscriptions": [{
            "name": "s",
            "sourceTopics": ["t"],
            "targetUrl": "https://example.com",
            "filter": "header:X-Tenant"
        }]}),
        serde_json::json!({"outboundSubscriptions": [{
            "name": "s",
            "sourceTopics": ["t"],
            "targetUrl": "https://example.com",
            "filter": "json:$["
        }]}),
    ] {
        let mut gw = gw_cr(GW);
        gw.spec = serde_json::from_value(invalid.clone()).expect("gateway spec");
        let rules = vec![MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkagrpcgateways/{GW}/status"),
            response: json_response(200, &fake_gateway_body(GW, NS, KAFKA)),
        }];
        let (ctx, state) = build_ctx(NS, rules);

        let result = reconcile(Arc::new(gw), ctx).await;
        assert!(result.is_err(), "accepted invalid {invalid}");

        let observed = state.take_observed();
        assert!(
            observed.len() == 1,
            "validation must happen before child reads/renders for {invalid}: {observed:?}"
        );
        let body: serde_json::Value =
            serde_json::from_slice(observed[0].body()).expect("status patch body");
        let ready = body["status"]["conditions"]
            .as_array()
            .and_then(|conditions| {
                conditions
                    .iter()
                    .find(|condition| condition["type"] == "Ready")
            })
            .expect("Ready condition");
        assert!(ready["status"] == "False", "body: {body}");
        assert!(ready["reason"] == "GatewayConfigInvalid", "body: {body}");
        assert!(state.remaining_rules() == 0);
    }
}

// ---------------------------------------------------------------------------
// Happy-path reconcile
// ---------------------------------------------------------------------------

/// Full reconcile: all child objects created from scratch, broker cert
/// already present (`KafkaUser` reconciler ran first), 1 replica ready.
///
/// Asserts:
/// - Returns `Action::requeue(30s)`.
/// - Deployment PATCH body carries exactly 5 volume mounts (serving,
///   broker-client, cluster-ca, clients-ca, config).
/// - Deployment PATCH body includes the broker-TLS args pointing at the
///   mounted paths.
/// - `KafkaUser` PATCH body has `authentication.type = "tls"`.
/// - Final status carries `Ready=True reason=Available`.
fn assert_ready_status(observed: &[http::Request<hyper::body::Bytes>]) {
    // --- Assert final status carries Ready=True / Available ---
    let status_patch = observed
        .iter()
        .rev()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkagrpcgateways/{GW}/status"))
        })
        .expect("gateway status PATCH must have been captured");
    let status_body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let conds = status_body["status"]["conditions"]
        .as_array()
        .expect("status must have conditions array");
    let ready = conds
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap_or_else(|| panic!("Ready condition missing; body = {status_body}"));
    check!(
        ready["status"] == "True",
        "Ready condition must be True when 1/1 replicas ready; body = {status_body}"
    );
    check!(
        ready["reason"] == "Available",
        "Ready reason must be Available; body = {status_body}"
    );
}

#[tokio::test]
async fn happy_path_all_objects_created_ready() {
    // Generate a real cluster CA so ensure_serving_cert can sign with it.
    let cluster_ca =
        crabka_security::ca::generate_cluster_ca("demo-cluster-ca", 365).expect("CA gen");

    let broker_user_name = format!("{GW}-broker");
    let serving_name = format!("{GW}-serving");
    let config_name = format!("{GW}-config");
    let cluster_ca_key_name = format!("{KAFKA}-cluster-ca");
    let cluster_ca_cert_name = format!("{KAFKA}-cluster-ca-cert");

    let rules = vec![
        // 1. GET parent Kafka → 200, version valid
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{KAFKA}"),
            response: json_response(200, &fake_parent_kafka_body(KAFKA, NS)),
        },
        // 2. PATCH child KafkaUser (SSA) → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{broker_user_name}"),
            response: json_response(200, &fake_kafkauser_body(&broker_user_name, NS)),
        },
        // 3. GET broker cert Secret → 200 (cert issued, don't wait)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{broker_user_name}"),
            response: json_response(200, &fake_broker_user_secret(GW, NS)),
        },
        // 4. GET serving cert Secret → 404 (first reconcile, need to issue)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{serving_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        // 5. GET cluster-CA key Secret → 200 (real PEM for signing)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key_name}"),
            response: json_response(
                200,
                &fake_cluster_ca_key_secret(KAFKA, NS, &cluster_ca.key_pem),
            ),
        },
        // 6. GET cluster-CA cert Secret → 200 (real PEM for signing)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert_name}"),
            response: json_response(
                200,
                &fake_cluster_ca_cert_secret(KAFKA, NS, &cluster_ca.cert_pem),
            ),
        },
        // 7. PATCH serving cert Secret (newly issued) → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{serving_name}"),
            response: json_response(200, &fake_serving_secret(GW, NS)),
        },
        // 8. PATCH config Secret → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{config_name}"),
            response: json_response(200, &fake_config_secret(GW, NS)),
        },
        // 9. PATCH Deployment → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/deployments/{GW}"),
            response: json_response(200, &fake_deployment_body(GW, NS, Some(1))),
        },
        // 10. PATCH Service → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{GW}"),
            response: json_response(200, &fake_service_body(GW, NS)),
        },
        // 11. GET Deployment (status read-back) → 200, 1/1 ready
        MockRule {
            method: Method::GET,
            path_substr: format!("/deployments/{GW}"),
            response: json_response(200, &fake_deployment_body(GW, NS, Some(1))),
        },
        // 12. PATCH gateway status → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkagrpcgateways/{GW}/status"),
            response: json_response(200, &fake_gateway_body(GW, NS, KAFKA)),
        },
    ];

    let (ctx, state) = build_ctx(NS, rules);
    let gw = gw_cr(GW);
    let action = reconcile(Arc::new(gw), ctx).await.expect("reconcile ok");

    // Action must be a 30-second requeue.
    assert!(
        action == kube::runtime::controller::Action::requeue(std::time::Duration::from_secs(30)),
        "expected requeue(30s), got {action:?}"
    );

    let observed = state.take_observed();

    // --- Assert KafkaUser PATCH body has TLS authentication ---
    let user_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkausers/{broker_user_name}"))
        })
        .expect("KafkaUser PATCH must have been captured");
    let user_body: serde_json::Value =
        serde_json::from_slice(user_patch.body()).expect("KafkaUser PATCH body is JSON");
    let auth_type = user_body
        .pointer("/spec/authentication/type")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            panic!("KafkaUser PATCH missing spec.authentication.type; body = {user_body}")
        });
    assert!(
        auth_type == "tls",
        "KafkaUser must have authentication.type=tls; got {auth_type}"
    );

    // --- Assert Deployment PATCH body has exactly 5 volume mounts ---
    let dep_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains(&format!("/deployments/{GW}"))
        })
        .expect("Deployment PATCH must have been captured");
    let dep_body: serde_json::Value =
        serde_json::from_slice(dep_patch.body()).expect("Deployment PATCH body is JSON");
    let mounts = dep_body
        .pointer("/spec/template/spec/containers/0/volumeMounts")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("Deployment PATCH missing volumeMounts; body = {dep_body}"));
    let mount_names: Vec<&str> = mounts
        .iter()
        .filter_map(|m| m.get("name").and_then(|v| v.as_str()))
        .collect();
    assert!(
        mounts.len() == 5,
        "Deployment must have exactly 5 volumeMounts, got {}: {:?}",
        mounts.len(),
        mount_names
    );
    for want in [
        "serving",
        "broker-client",
        "cluster-ca",
        "clients-ca",
        "config",
    ] {
        assert!(
            mount_names.contains(&want),
            "Deployment volumeMounts must include '{want}'; got {mount_names:?}"
        );
    }

    // --- Assert Deployment PATCH body has broker-TLS CLI args ---
    let args = dep_body
        .pointer("/spec/template/spec/containers/0/args")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("Deployment PATCH missing args; body = {dep_body}"));
    let args_joined: String = args
        .iter()
        .filter_map(|a| a.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    // The broker-TLS cert/key/CA and serving-cert args must point at the
    // mounted paths. The parent fixture exposes a plaintext PLAIN:9092 AND a
    // TLS+mTLS `tls-internal`:9093 — the gateway must dial the SECURED
    // listener (port 9093), never the plaintext :9092, and its SNI must be
    // the broker headless-svc SAN.
    for (needle, want) in [
        (
            "--broker-tls-cert=/etc/crabka-gw/broker-client/user.crt",
            true,
        ),
        (
            "--broker-tls-key=/etc/crabka-gw/broker-client/user.key",
            true,
        ),
        ("--broker-tls-ca=/etc/crabka-gw/cluster-ca/ca.crt", true),
        ("--tls-cert=/etc/crabka-gw/serving/tls.crt", true),
        (
            "--bootstrap-servers=demo-broker-headless.default.svc.cluster.local:9093",
            true,
        ),
        (
            "--bootstrap-servers=demo-broker-headless.default.svc.cluster.local:9092",
            false,
        ),
        (
            "--broker-tls-server-name=demo-broker-headless.default.svc.cluster.local",
            true,
        ),
    ] {
        assert!(
            args_joined.contains(needle) == want,
            "Deployment arg fragment {needle:?}: expected present={want}; args = {args_joined}"
        );
    }

    assert_ready_status(&observed);

    // All 12 rules must have been consumed.
    check!(
        state.remaining_rules() == 0,
        "all FIFO rules must be consumed, {} remaining",
        state.remaining_rules()
    );
}

// ---------------------------------------------------------------------------
// No-TLS-listener degraded path
// ---------------------------------------------------------------------------

/// Parent Kafka exposes ONLY a plaintext `PLAIN` listener (no TLS + mTLS
/// internal listener). The gateway requires full mTLS to the broker, so
/// reconcile must surface `Ready=False reason=NoTlsListener`, requeue, and
/// render NO Deployment / Service.
///
/// The reconcile still walks the child-KafkaUser + serving-cert + config-Secret
/// steps (those don't depend on the broker endpoint), then bails at step (6)
/// when `resolve_broker_endpoint` returns `None`.
#[tokio::test]
async fn no_tls_listener_blocks_with_degraded_and_no_deployment() {
    let cluster_ca =
        crabka_security::ca::generate_cluster_ca("demo-cluster-ca", 365).expect("CA gen");

    let broker_user_name = format!("{GW}-broker");
    let serving_name = format!("{GW}-serving");
    let config_name = format!("{GW}-config");
    let cluster_ca_key_name = format!("{KAFKA}-cluster-ca");
    let cluster_ca_cert_name = format!("{KAFKA}-cluster-ca-cert");

    // A validated parent with only a plaintext PLAIN listener — no TLS+mTLS
    // listener for the gateway to dial.
    let plain_only_kafka = serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": KAFKA, "namespace": NS, "uid": "kafka-uid" },
        "spec": {
            "kafkaVersion": "0.1.1",
            "listeners": [
                { "name": "PLAIN", "port": 9092, "type": "internal", "tls": false }
            ]
        },
        "status": {
            "conditions": [{
                "type": "KafkaVersionValid",
                "status": "True",
                "reason": "Valid",
                "message": "ok",
                "lastTransitionTime": "2026-05-22T00:00:00Z"
            }],
            "metadataVersion": "0.1",
            "listeners": [
                {
                    "name": "PLAIN",
                    "type": "internal",
                    "bootstrapServers": format!("{KAFKA}-broker-headless.{NS}.svc.cluster.local:9092")
                }
            ]
        }
    });

    let rules = vec![
        // 1. GET parent Kafka → validated, plaintext-only
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{KAFKA}"),
            response: json_response(200, &plain_only_kafka),
        },
        // 2. PATCH child KafkaUser (SSA) → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkausers/{broker_user_name}"),
            response: json_response(200, &fake_kafkauser_body(&broker_user_name, NS)),
        },
        // 3. GET broker cert Secret → 200 (cert issued, don't wait)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{broker_user_name}"),
            response: json_response(200, &fake_broker_user_secret(GW, NS)),
        },
        // 4. GET serving cert Secret → 404 (first reconcile, need to issue)
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{serving_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404 builds"),
        },
        // 5. GET cluster-CA key Secret → 200
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key_name}"),
            response: json_response(
                200,
                &fake_cluster_ca_key_secret(KAFKA, NS, &cluster_ca.key_pem),
            ),
        },
        // 6. GET cluster-CA cert Secret → 200
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert_name}"),
            response: json_response(
                200,
                &fake_cluster_ca_cert_secret(KAFKA, NS, &cluster_ca.cert_pem),
            ),
        },
        // 7. PATCH serving cert Secret → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{serving_name}"),
            response: json_response(200, &fake_serving_secret(GW, NS)),
        },
        // 8. PATCH config Secret → 200
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{config_name}"),
            response: json_response(200, &fake_config_secret(GW, NS)),
        },
        // 9. PATCH gateway status (NoTlsListener degraded early return)
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkagrpcgateways/{GW}/status"),
            response: json_response(200, &fake_gateway_body(GW, NS, KAFKA)),
        },
        // No Deployment / Service rules: reconcile must bail before them.
    ];

    let (ctx, state) = build_ctx(NS, rules);
    let gw = gw_cr(GW);
    let action = reconcile(Arc::new(gw), ctx).await.expect("reconcile ok");

    assert!(
        action == kube::runtime::controller::Action::requeue(std::time::Duration::from_secs(30)),
        "expected requeue(30s) from no-TLS-listener gate, got {action:?}"
    );

    let observed = state.take_observed();

    // Status PATCH must carry Ready=False reason=NoTlsListener.
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkagrpcgateways/{GW}/status"))
        })
        .expect("gateway status PATCH must have been captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("status must have conditions array");
    let ready = conds
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap_or_else(|| panic!("Ready condition missing; body = {body}"));
    assert!(
        ready["status"] == "False",
        "Ready must be False without a TLS listener; body = {body}"
    );
    assert!(
        ready["reason"] == "NoTlsListener",
        "reason must be NoTlsListener; body = {body}"
    );

    // No Deployment or Service PATCH must have happened.
    let unexpected: Vec<_> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && (r.uri().to_string().contains("/deployments/")
                    || r.uri().to_string().contains("/services/"))
        })
        .map(|r| r.uri().to_string())
        .collect();
    assert!(
        unexpected.is_empty(),
        "no-TLS-listener gate must prevent Deployment/Service render; unexpected PATCHes: {unexpected:?}"
    );

    assert!(
        state.remaining_rules() == 0,
        "all FIFO rules must be consumed, {} remaining",
        state.remaining_rules()
    );
}

// ---------------------------------------------------------------------------
// Version-gate early-return
// ---------------------------------------------------------------------------

/// Parent Kafka has no `KafkaVersionValid` condition and no
/// `status.metadataVersion` → reconcile must patch `Ready=False
/// reason=WaitingForVersionValidation` on the gateway and return without
/// creating any child objects (Deployment, Service, `KafkaUser`, Secrets).
#[tokio::test]
async fn version_gate_blocks_when_kafka_not_validated() {
    // A Kafka body with no version conditions and no metadataVersion.
    let unvalidated_kafka = serde_json::json!({
        "apiVersion": "crabka.io/v1alpha1",
        "kind": "Kafka",
        "metadata": { "name": KAFKA, "namespace": NS, "uid": "kafka-uid" },
        "spec": { "kafkaVersion": "0.1.1" },
        "status": {
            "conditions": [],
            // no metadataVersion → version_gate returns Some(cond) → early return
        }
    });

    let rules = vec![
        // 1. GET parent Kafka → unvalidated
        MockRule {
            method: Method::GET,
            path_substr: format!("/kafkas/{KAFKA}"),
            response: json_response(200, &unvalidated_kafka),
        },
        // 2. PATCH gateway status (version gate early return)
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkagrpcgateways/{GW}/status"),
            response: json_response(200, &fake_gateway_body(GW, NS, KAFKA)),
        },
        // No further rules: the reconcile must return after the status PATCH.
    ];

    let (ctx, state) = build_ctx(NS, rules);
    let gw = gw_cr(GW);
    let action = reconcile(Arc::new(gw), ctx).await.expect("reconcile ok");

    // Returns a 30-second requeue (waiting for version validation).
    assert!(
        action == kube::runtime::controller::Action::requeue(std::time::Duration::from_secs(30)),
        "expected requeue(30s) from version gate, got {action:?}"
    );

    let observed = state.take_observed();

    // The status PATCH must carry WaitingForVersionValidation.
    let status_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/kafkagrpcgateways/{GW}/status"))
        })
        .expect("gateway status PATCH must have been captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status PATCH body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("status must have conditions array");
    let ready = conds
        .iter()
        .find(|c| c["type"] == "Ready")
        .unwrap_or_else(|| panic!("Ready condition missing; body = {body}"));
    assert!(
        ready["status"] == "False",
        "Ready must be False when version-gated; body = {body}"
    );
    assert!(
        ready["reason"] == "WaitingForVersionValidation",
        "reason must be WaitingForVersionValidation; body = {body}"
    );

    // NO Deployment, Service, or KafkaUser PATCHes must have happened.
    let unexpected: Vec<_> = observed
        .iter()
        .filter(|r| {
            r.method() == Method::PATCH
                && (r.uri().to_string().contains("/deployments/")
                    || r.uri().to_string().contains("/services/")
                    || r.uri().to_string().contains("/kafkausers/"))
        })
        .map(|r| r.uri().to_string())
        .collect();
    assert!(
        unexpected.is_empty(),
        "version gate must prevent child object creation; unexpected PATCHes: {unexpected:?}"
    );

    // Both rules consumed (GET + PATCH status).
    assert!(
        state.remaining_rules() == 0,
        "all FIFO rules must be consumed, {} remaining",
        state.remaining_rules()
    );
}
