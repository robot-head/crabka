//! Integration tests for listener authentication wiring — SCRAM-SHA-512, SCRAM-SHA-256, mTLS, and `NodePort` SAN injection.

use std::sync::Arc;

use assert2::{assert, check};
use crabka_operator::{
    controller::kafka::reconcile,
    crd::{Kafka, KafkaSpec, Listener, ListenerAuthentication, ListenerType},
};
use http::{Method, Response};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, build_ctx, extract_broker0_toml, fake_ca_secret, fake_kafka_body,
    fake_keystore_secret, fake_pool_body, fake_pool_list_body, fake_pool_list_item,
    fake_secret_body, fake_service_body, happy_path_rules, json_response, not_found_body,
};

// ── helpers ──────────────────────────────────────────────────────────────────

// fake_ca_secret, fake_keystore_secret, happy_path_rules, build_ctx are in shared/mod.rs.

fn kafka_cr_with_listeners(name: &str, namespace: &str, listeners: Vec<Listener>) -> Kafka {
    let mut k = Kafka::new(
        name,
        KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners,
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy: None,
            cluster_ca: None,
            clients_ca: None,
            logging: None,
            delegation_token: None,
            authorization: None,
            tiered_storage: None,
            inter_broker_kerberos: None,
            krb5_conf_secret_ref: None,
            tracing: None,
        },
    );
    k.metadata.namespace = Some(namespace.into());
    k.metadata.uid = Some("kafka-uid".into());
    k
}

fn internal_listener(
    name: &str,
    port: i32,
    tls: bool,
    auth: Option<ListenerAuthentication>,
) -> Listener {
    Listener {
        name: name.into(),
        port,
        type_: ListenerType::Internal,
        tls,
        authentication: auth,
        configuration: None,
        network_policy_peers: None,
    }
}

#[tokio::test]
async fn internal_listener_auth_render_cases() {
    for (case, namespace, cluster, listener, expected_fragments) in [
        (
            "SCRAM-SHA-512 with TLS",
            "ns1",
            "c1",
            internal_listener(
                "data",
                9094,
                true,
                Some(ListenerAuthentication::ScramSha512),
            ),
            vec![
                "protocol = \"SaslSsl\"",
                "tls_config = {",
                "sasl_config = { enabled_mechanisms = [\"SCRAM-SHA-512\"] }",
            ],
        ),
        (
            "mTLS client authentication",
            "ns2",
            "c2",
            internal_listener("mtls", 9095, true, Some(ListenerAuthentication::Tls)),
            vec![
                "protocol = \"Ssl\"",
                "client_ca_path = \"/etc/crabka/clients-ca/ca.crt\"",
                "client_auth = \"Required\"",
            ],
        ),
    ] {
        let items = vec![fake_pool_list_item("brokers", namespace, cluster, 1, 1)];
        let (ctx, state) = build_ctx(namespace, happy_path_rules(cluster, namespace, &items));
        let kafka = kafka_cr_with_listeners(cluster, namespace, vec![listener]);
        reconcile(Arc::new(kafka), ctx)
            .await
            .unwrap_or_else(|error| panic!("{case}: reconcile failed: {error}"));

        let observed = state.take_observed();
        let toml = extract_broker0_toml(&observed, cluster);
        for fragment in expected_fragments {
            assert!(
                toml.contains(fragment),
                "{case}: expected {fragment:?};\n{toml}"
            );
        }
    }
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// SCRAM-SHA-256 internal listener renders `protocol = "SaslSsl"` and
/// `enabled_mechanisms = ["SCRAM-SHA-256"]`.
#[tokio::test]
async fn scram_sha_256_renders_sasl_ssl_with_256_mechanism() {
    let items = vec![fake_pool_list_item("brokers", "ns3", "c3", 1, 1)];
    let (ctx, state) = build_ctx("ns3", happy_path_rules("c3", "ns3", &items));

    let kafka = kafka_cr_with_listeners(
        "c3",
        "ns3",
        vec![internal_listener(
            "scram256",
            9094,
            true,
            Some(ListenerAuthentication::ScramSha256),
        )],
    );
    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();
    let toml = extract_broker0_toml(&observed, "c3");

    assert!(
        toml.contains("protocol = \"SaslSsl\""),
        "expected SaslSsl for SCRAM-SHA-256 with TLS;\n{toml}"
    );
    assert!(
        toml.contains("sasl_config = { enabled_mechanisms = [\"SCRAM-SHA-256\"] }"),
        "expected SCRAM-SHA-256 mechanism;\n{toml}"
    );
}

// ── test 4 ────────────────────────────────────────────────────────────────────

/// A SCRAM-SHA-512 listener with zero `KafkaUser`s reconciles cleanly.
/// The Kafka reconciler never consults `KafkaUser`s — those are handled by the
/// `KafkaUser` reconciler — so an empty cluster is not an error.
#[tokio::test]
async fn scram_listener_without_kafkausers_still_reconciles() {
    let items = vec![fake_pool_list_item("brokers", "ns4", "c4", 1, 1)];
    let (ctx, _state) = build_ctx("ns4", happy_path_rules("c4", "ns4", &items));

    let kafka = kafka_cr_with_listeners(
        "c4",
        "ns4",
        vec![internal_listener(
            "scram",
            9094,
            true,
            Some(ListenerAuthentication::ScramSha512),
        )],
    );

    // Must not error — empty SCRAM credential set is valid.
    reconcile(Arc::new(kafka), ctx).await.unwrap();
}

// ── test 5 ────────────────────────────────────────────────────────────────────

/// mTLS without transport TLS is an invalid combination.
/// The status PATCH must carry `ListenersValid=False` with reason
/// `ListenerMtlsRequiresTransportTls`, and no `ConfigMap` PATCH must occur.
#[tokio::test]
async fn listener_mtls_requires_tls_validation_error_surfaces_status() {
    let items = vec![fake_pool_list_item("brokers", "ns5", "c5", 1, 1)];
    let mut rules = happy_path_rules("c5", "ns5", &items);
    // On validation failure, the ConfigMap and broker-keystore are skipped.
    rules.retain(|r| !r.path_substr.contains("/configmaps/"));
    rules.retain(|r| !r.path_substr.contains("-kafka-brokers"));
    let (ctx, state) = build_ctx("ns5", rules);

    let kafka = kafka_cr_with_listeners(
        "c5",
        "ns5",
        vec![Listener {
            name: "bad".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            authentication: Some(ListenerAuthentication::Tls),
            configuration: None,
            network_policy_peers: None,
        }],
    );

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // ConfigMap PATCH must be absent.
    assert!(
        !observed.iter().any(|r| {
            r.method() == Method::PATCH
                && r.uri().to_string().contains("/configmaps/c5-broker-config")
        }),
        "validation failure must not patch the broker-config ConfigMap"
    );

    // Status must surface the validation error.
    let status_patch = observed
        .iter()
        .find(|r| r.method() == Method::PATCH && r.uri().to_string().contains("/kafkas/c5/status"))
        .expect("status PATCH captured");
    let body: serde_json::Value =
        serde_json::from_slice(status_patch.body()).expect("status body is JSON");
    let conds = body["status"]["conditions"]
        .as_array()
        .expect("conditions array");

    let valid = conds
        .iter()
        .find(|c| c["type"] == "ListenersValid")
        .unwrap_or_else(|| panic!("ListenersValid present; body = {body}"));
    assert_eq!(
        (valid["status"].as_str(), valid["reason"].as_str()),
        (Some("False"), Some("ListenerMtlsRequiresTransportTls")),
        "body = {body}"
    );

    check!(state.remaining_rules() == 0);
}

// ── test 6 ────────────────────────────────────────────────────────────────────

/// Changing auth config (SCRAM-SHA-512 → mTLS) produces a different
/// `crabka.io/config-hash` label on the pool owner-ref PATCH.
#[tokio::test]
async fn auth_change_bumps_config_hash() {
    // First reconcile: SCRAM-SHA-512.
    let items = vec![fake_pool_list_item("brokers", "ns6", "c6", 1, 1)];
    let (ctx1, state1) = build_ctx("ns6", happy_path_rules("c6", "ns6", &items));
    let kafka_scram = kafka_cr_with_listeners(
        "c6",
        "ns6",
        vec![internal_listener(
            "data",
            9094,
            true,
            Some(ListenerAuthentication::ScramSha512),
        )],
    );
    reconcile(Arc::new(kafka_scram), ctx1).await.unwrap();
    let observed1 = state1.take_observed();

    let pool_patch1 = observed1
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkanodepools/brokers")
        })
        .expect("pool PATCH captured in first reconcile");
    let body1: serde_json::Value =
        serde_json::from_slice(pool_patch1.body()).expect("pool PATCH body is JSON");
    let hash1 = body1["metadata"]["labels"]["crabka.io/config-hash"]
        .as_str()
        .unwrap_or_else(|| panic!("config-hash label missing; body = {body1}"))
        .to_string();

    // Second reconcile: mTLS (different auth → different TOML → different hash).
    let items2 = vec![fake_pool_list_item("brokers", "ns6b", "c6b", 1, 1)];
    let (ctx2, state2) = build_ctx("ns6b", happy_path_rules("c6b", "ns6b", &items2));
    let kafka_mtls = kafka_cr_with_listeners(
        "c6b",
        "ns6b",
        vec![internal_listener(
            "data",
            9094,
            true,
            Some(ListenerAuthentication::Tls),
        )],
    );
    reconcile(Arc::new(kafka_mtls), ctx2).await.unwrap();
    let observed2 = state2.take_observed();

    let pool_patch2 = observed2
        .iter()
        .find(|r| {
            r.method() == Method::PATCH && r.uri().to_string().contains("/kafkanodepools/brokers")
        })
        .expect("pool PATCH captured in second reconcile");
    let body2: serde_json::Value =
        serde_json::from_slice(pool_patch2.body()).expect("pool PATCH body is JSON");
    let hash2 = body2["metadata"]["labels"]["crabka.io/config-hash"]
        .as_str()
        .unwrap_or_else(|| panic!("config-hash label missing; body = {body2}"))
        .to_string();

    assert!(
        hash1 != hash2,
        "config-hash must differ between SCRAM-SHA-512 and mTLS configs"
    );

    // Both hashes must be valid 16-char hex strings.
    for (hash, label) in [(&hash1, "scram"), (&hash2, "mtls")] {
        assert!(
            hash.len() == 16,
            "{label} config-hash must be 16 hex chars, got {hash:?}"
        );
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "{label} config-hash must be hex, got {hash:?}"
        );
    }
}

// ── test 7 ────────────────────────────────────────────────────────────────────

/// A `NodePort` listener with TLS + SCRAM causes `observe_listener_addresses`
/// to GET the node list. The external IP observed from a node with
/// `ExternalIP=203.0.113.10` must be included in the per-broker cert's SAN
/// set, as evidenced by the `0.sans-digest` stored in the keystore Secret
/// PATCH matching the digest computed from SANs that include that IP.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn nodeport_listener_external_san_added_to_per_broker_cert() {
    use base64::Engine as _;
    use crabka_security::ca::SubjectAltName;

    let ns = "ns7";
    let name = "c7";
    let pool_name = "brokers";

    let items = vec![fake_pool_list_item(pool_name, ns, name, 1, 1)];
    let svc_name = format!("{name}-broker-headless");
    let secret_name = format!("{name}-cluster-id");
    let cluster_ca_key = format!("{name}-cluster-ca");
    let cluster_ca_cert = format!("{name}-cluster-ca-cert");
    let clients_ca_key = format!("{name}-clients-ca");
    let clients_ca_cert = format!("{name}-clients-ca-cert");
    let keystore_name = format!("{name}-kafka-brokers");

    // The external listener name — used for per-broker + bootstrap Service names.
    let ext_listener_name = "external";
    let bootstrap_svc = format!("{name}-{ext_listener_name}-bootstrap");
    let broker_svc = format!("{name}-{ext_listener_name}-0");
    let ext_node_ip = "203.0.113.10";

    // Node list response: one node with ExternalIP.
    let node_list_response = serde_json::json!({
        "kind": "NodeList",
        "apiVersion": "v1",
        "metadata": { "resourceVersion": "1" },
        "items": [{
            "metadata": { "name": "node1" },
            "status": {
                "addresses": [
                    { "type": "ExternalIP", "address": ext_node_ip }
                ]
            }
        }]
    });

    // Pod list response: empty (no pods scheduled → PendingExternalAddresses).
    let pod_list_response = serde_json::json!({
        "kind": "PodList",
        "apiVersion": "v1",
        "metadata": { "resourceVersion": "1" },
        "items": []
    });

    let fake_bootstrap_svc = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": bootstrap_svc, "namespace": ns, "uid": "bs-uid" },
        "spec": { "type": "NodePort", "ports": [{ "port": 9094, "nodePort": 30094 }] }
    });

    let fake_broker_svc = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": broker_svc, "namespace": ns, "uid": "bsvc-uid" },
        "spec": { "type": "NodePort", "ports": [{ "port": 9094, "nodePort": 30095 }] }
    });

    let mut rules = vec![
        // Headless service.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{svc_name}"),
            response: json_response(200, &fake_service_body(&svc_name, ns)),
        },
        // Cluster-id secret.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{secret_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::POST,
            path_substr: format!("/namespaces/{ns}/secrets"),
            response: json_response(
                201,
                &fake_secret_body(&secret_name, ns, "00000000-0000-0000-0000-000000000000"),
            ),
        },
        // Cluster CA.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_key}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_key, ns)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{cluster_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&cluster_ca_cert, ns)),
        },
        // Clients CA.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_key}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_key, ns)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{clients_ca_cert}"),
            response: json_response(200, &fake_ca_secret(&clients_ca_cert, ns)),
        },
        // Pool list.
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/kafkanodepools"),
            response: json_response(200, &fake_pool_list_body(&items)),
        },
        // observe_listener_addresses: GET nodes (nodeport+tls triggers this).
        MockRule {
            method: Method::GET,
            path_substr: "/api/v1/nodes".into(),
            response: json_response(200, &node_list_response),
        },
        // Broker keystore.
        MockRule {
            method: Method::GET,
            path_substr: format!("/secrets/{keystore_name}"),
            response: Response::builder()
                .status(404)
                .header("content-type", "application/json")
                .body(not_found_body("not found"))
                .expect("404"),
        },
        // The keystore PATCH is what we'll inspect for the SAN digest.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/secrets/{keystore_name}"),
            response: json_response(200, &fake_keystore_secret(&keystore_name, ns)),
        },
        // apply_external_services: PATCH bootstrap + per-broker services.
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{bootstrap_svc}"),
            response: json_response(200, &fake_bootstrap_svc),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/services/{broker_svc}"),
            response: json_response(200, &fake_broker_svc),
        },
        // read_external_state: GET nodes again.
        MockRule {
            method: Method::GET,
            path_substr: "/api/v1/nodes".into(),
            response: json_response(200, &node_list_response),
        },
        // read_external_state: GET pods (empty list → no pods scheduled).
        MockRule {
            method: Method::GET,
            path_substr: format!("/namespaces/{ns}/pods"),
            response: json_response(200, &pod_list_response),
        },
        // Pool adopt + status patch (PendingExternalAddresses; no CM patch).
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkanodepools/{pool_name}?"),
            response: json_response(200, &fake_pool_body(pool_name, ns, name)),
        },
        MockRule {
            method: Method::PATCH,
            path_substr: format!("/kafkas/{name}/status"),
            response: json_response(200, &fake_kafka_body(name, ns)),
        },
    ];

    // read_external_state also does get_opt for per-broker and bootstrap services
    // (NodePort only, not Loadbalancer). get_opt on 404 returns None — the FIFO
    // fallback 404 is fine here since get_opt does not propagate 404 as an error.
    // However, the FIFO mock *does* record the unmatched requests. Add explicit
    // rules so the fallback 404 doesn't consume the "unexpected" codepath.
    rules.push(MockRule {
        method: Method::GET,
        path_substr: format!("/services/{broker_svc}"),
        response: json_response(200, &fake_broker_svc),
    });
    rules.push(MockRule {
        method: Method::GET,
        path_substr: format!("/services/{bootstrap_svc}"),
        response: json_response(200, &fake_bootstrap_svc),
    });

    let (ctx, state) = build_ctx(ns, rules);

    // A NodePort-only cluster is invalid (NoInternalListener). Add a plain
    // internal listener so validation passes.
    let kafka = kafka_cr_with_listeners(
        name,
        ns,
        vec![
            Listener {
                name: "internal".into(),
                port: 9092,
                type_: ListenerType::Internal,
                tls: false,
                authentication: None,
                configuration: None,
                network_policy_peers: None,
            },
            Listener {
                name: ext_listener_name.into(),
                port: 9094,
                type_: ListenerType::Nodeport,
                tls: true,
                authentication: Some(ListenerAuthentication::ScramSha512),
                configuration: None,
                network_policy_peers: None,
            },
        ],
    );

    reconcile(Arc::new(kafka), ctx).await.unwrap();

    let observed = state.take_observed();

    // Find the keystore PATCH.
    let ks_patch = observed
        .iter()
        .find(|r| {
            r.method() == Method::PATCH
                && r.uri()
                    .to_string()
                    .contains(&format!("/secrets/{keystore_name}"))
        })
        .unwrap_or_else(|| {
            panic!(
                "broker keystore PATCH not found; observed: {:?}",
                observed
                    .iter()
                    .map(|r| format!("{} {}", r.method(), r.uri()))
                    .collect::<Vec<_>>()
            )
        });

    let ks_body: serde_json::Value =
        serde_json::from_slice(ks_patch.body()).expect("keystore PATCH body is JSON");

    // The keystore body is a full Secret SSA body; the `data` field holds
    // base64-encoded values.
    let data = ks_body
        .get("data")
        .and_then(|d| d.as_object())
        .unwrap_or_else(|| panic!("keystore PATCH has no data object; body = {ks_body}"));

    let digest_b64 = data
        .get("0.sans-digest")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            panic!(
                "0.sans-digest key missing from keystore PATCH; keys = {:?}",
                data.keys().collect::<Vec<_>>()
            )
        });

    // Decode and reconstruct the expected digest.
    let digest_bytes = base64::engine::general_purpose::STANDARD
        .decode(digest_b64)
        .expect("0.sans-digest is valid base64");
    let stored_digest = std::str::from_utf8(&digest_bytes).expect("digest is utf-8");

    // Base SANs the operator builds for broker 0:
    //   pod_fqdn, pod_name, headless svc FQDN, 127.0.0.1
    // Extra SANs from the NodePort external IP:
    //   203.0.113.10
    let cluster_name = name;
    let cluster_ns = ns;
    let pool_n = pool_name;
    let pod_name = format!("{cluster_name}-{pool_n}-0");
    let headless_svc = format!("{cluster_name}-broker-headless");
    let pod_fqdn = format!("{pod_name}.{headless_svc}.{cluster_ns}.svc.cluster.local");

    let base_sans = vec![
        SubjectAltName::Dns(pod_fqdn),
        SubjectAltName::Dns(pod_name),
        SubjectAltName::Dns(format!(
            "{cluster_name}-broker-headless.{cluster_ns}.svc.cluster.local"
        )),
        SubjectAltName::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    ];
    let extra_sans = vec![SubjectAltName::Ip(ext_node_ip.parse().expect("valid IP"))];

    let expected_digest =
        crabka_operator::controller::cluster_ca::compute_san_digest(&base_sans, &extra_sans);

    // Verifies the digest in the Secret matches the expected SAN set (including the node's
    // ExternalIP). This proves the SAN computation reached the keystore-write path, but does
    // not parse the cert PEM itself — issue_broker_cert is independently tested in
    // security/src/ca.rs and operator/src/controller/cluster_ca.rs::san_tests.
    assert!(
        stored_digest == expected_digest,
        "keystore 0.sans-digest must include the NodePort external IP {ext_node_ip}"
    );
}
