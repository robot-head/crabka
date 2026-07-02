// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.
#![allow(clippy::pedantic)]

//! KIP-714 client-metrics telemetry handshake, push, and error-path coverage.
//!
//! Crabka implements the full KIP-714 receiver. The broker:
//!   - Assigns a fresh `client_instance_id` when the caller sends nil; echoes
//!     nil when the caller sends a non-nil id.
//!   - Returns `accepted_compression_types = [4,3,1,2]` (ZSTD,LZ4,GZIP,SNAPPY),
//!     `telemetry_max_bytes = 1_048_576`, `delta_temporality = true`.
//!   - With no subscriptions configured: `requested_metrics` is empty and
//!     `push_interval_ms = 300_000`.
//!   - With a matching subscription: `requested_metrics` reflects the matched
//!     prefix set and `push_interval_ms` = min matched interval.
//!   - `PushTelemetry` from an unknown/unregistered instance → `error_code 42`
//!     (INVALID_REQUEST).
//!   - `PushTelemetry` with a stale `subscription_id` → `error_code 117`
//!     (UNKNOWN_SUBSCRIPTION_ID).
//!   - `PushTelemetry` with an unsupported `compression_type` → `error_code 76`
//!     (UNSUPPORTED_COMPRESSION_TYPE).
//!   - Valid push → `error_code 0`.
//!
//! Tests using `IncrementalAlterConfigs` (to set up subscriptions) require a
//! controller-backed single-node cluster (`start_n_node(1)`) because that RPC
//! goes through Raft. Simple handshake tests use `support::start()` (simpler
//! single-broker helper that also boots in Bootstrap mode and is its own
//! controller).

#![allow(clippy::default_trait_access)]

use assert2::{assert, check};
mod support;

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest;
use crabka_protocol::owned::get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse;
use crabka_protocol::owned::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
};
use crabka_protocol::owned::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use crabka_protocol::owned::push_telemetry_request::PushTelemetryRequest;
use crabka_protocol::owned::push_telemetry_response::PushTelemetryResponse;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use support::start_n_node;

/// Kafka resource type id for CLIENT_METRICS (KIP-714).
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;

/// `config_operation` SET = 0 in the IncrementalAlterConfigs wire protocol.
const CONFIG_OP_SET: i8 = 0;

// ── helpers ───────────────────────────────────────────────────────────────────

async fn build_client(addr: std::net::SocketAddr) -> crabka_client_core::Client {
    crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", addr.port()))
        .client_id("client-telemetry-test")
        .build()
        .await
        .expect("client build")
}

/// Configure a match-all CLIENT_METRICS subscription via IncrementalAlterConfigs.
async fn configure_match_all_subscription(
    client: &crabka_client_core::Client,
    name: &str,
    interval_ms: &str,
) {
    let alter_req = IncrementalAlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: name.to_string(),
            configs: vec![
                AlterableConfig {
                    name: "metrics".to_string(),
                    config_operation: CONFIG_OP_SET,
                    value: Some("*".to_string()),
                    ..Default::default()
                },
                AlterableConfig {
                    name: "interval.ms".to_string(),
                    config_operation: CONFIG_OP_SET,
                    value: Some(interval_ms.to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };

    let alter_resp: IncrementalAlterConfigsResponse = client
        .send(alter_req)
        .await
        .expect("IncrementalAlterConfigs");

    assert!(
        alter_resp.responses.len() == 1,
        "expected one resource response, got {}",
        alter_resp.responses.len()
    );
    let r = &alter_resp.responses[0];
    assert!(
        r.error_code == 0,
        "IncrementalAlterConfigs CLIENT_METRICS must succeed; error_code={} message={:?}",
        r.error_code,
        r.error_message,
    );
}

/// Build a minimal valid OTLP MetricsData payload (uncompressed / compression_type=0).
fn sample_otlp_metrics() -> bytes::Bytes {
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data,
        number_data_point::Value,
    };
    use prost::Message;
    let dp = NumberDataPoint {
        value: Some(Value::AsInt(7)),
        ..Default::default()
    };
    let metric = Metric {
        name: "org.apache.kafka.consumer.fetch.size".into(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![dp],
        })),
        ..Default::default()
    };
    let md = MetricsData {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![metric],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    bytes::Bytes::from(md.encode_to_vec())
}

// ── Part 1: fixed legacy tests ────────────────────────────────────────────────

#[tokio::test]
async fn api_versions_advertises_telemetry_apis() {
    let p = support::start().await;
    let resp = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");

    let advertised: std::collections::HashSet<i16> =
        resp.api_keys.iter().map(|k| k.api_key).collect();
    assert!(
        advertised.contains(&71),
        "ApiVersions must advertise GetTelemetrySubscriptions (71), got {advertised:?}",
    );
    assert!(
        advertised.contains(&72),
        "ApiVersions must advertise PushTelemetry (72), got {advertised:?}",
    );

    p.broker.shutdown().await;
}

/// With no subscriptions configured the broker assigns a fresh id, returns
/// empty `requested_metrics`, and advertises the standard compression types
/// and limits.
#[tokio::test]
async fn get_telemetry_subscriptions_with_nil_id_returns_assigned_id_and_no_subscription() {
    let p = support::start().await;

    let resp = p
        .client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: WireUuid::ZERO,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    check!(resp.error_code == 0, "handler must succeed: {resp:?}");
    check!(
        resp.client_instance_id != WireUuid::ZERO,
        "broker must assign a fresh client_instance_id when caller sent nil"
    );
    // No subscriptions configured → empty requested_metrics (the "don't push" signal).
    check!(
        resp.requested_metrics.is_empty(),
        "no subscription configured → requested_metrics must be empty, got {:?}",
        resp.requested_metrics,
    );
    // Standard KIP-714 compression advertisement: ZSTD(4), LZ4(3), GZIP(1), SNAPPY(2).
    check!(
        resp.accepted_compression_types == vec![4i8, 3, 1, 2],
        "accepted_compression_types must be [4,3,1,2], got {:?}",
        resp.accepted_compression_types,
    );
    check!(
        resp.telemetry_max_bytes == 1_048_576,
        "telemetry_max_bytes must be 1 MiB, got {}",
        resp.telemetry_max_bytes,
    );
    check!(resp.delta_temporality, "delta_temporality must be true",);
    // Default interval when no subscription is matched: 300 000 ms (5 min).
    check!(
        resp.push_interval_ms == 300_000,
        "push_interval_ms must be 300_000 when no subscription configured, got {}",
        resp.push_interval_ms,
    );

    p.broker.shutdown().await;
}

/// Non-nil request id must round-trip as nil per the KIP-714 schema rule:
/// "Assigned client instance id if ClientInstanceId was 0 in the request,
/// else 0."
#[tokio::test]
async fn get_telemetry_subscriptions_with_set_id_echoes_nil() {
    let p = support::start().await;

    let prior_id = WireUuid([0x11; 16]);
    let resp = p
        .client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: prior_id,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    assert!(resp.error_code == 0);
    assert!(
        resp.client_instance_id == WireUuid::ZERO,
        "non-nil request id must round-trip as nil per schema rules"
    );

    p.broker.shutdown().await;
}

/// An unregistered instance pushing telemetry must be rejected with
/// INVALID_REQUEST (42) — the client should call GetTelemetrySubscriptions
/// first.
#[tokio::test]
async fn push_telemetry_unknown_instance_rejected() {
    let p = support::start().await;

    let resp: PushTelemetryResponse = p
        .client
        .send(PushTelemetryRequest {
            client_instance_id: WireUuid([0x22; 16]),
            subscription_id: 0,
            terminating: false,
            compression_type: 0,
            metrics: bytes::Bytes::from_static(b"\x00\x01\x02"),
            ..Default::default()
        })
        .await
        .expect("PushTelemetry");

    assert!(
        resp.error_code == 42,
        "unknown instance must be rejected with INVALID_REQUEST (42), got {}",
        resp.error_code,
    );

    p.broker.shutdown().await;
}

// ── Part 2: e2e coverage (controller-backed) ─────────────────────────────────

/// Happy path: configure a subscription, do a GetTelemetrySubscriptions
/// handshake, then push a valid OTLP payload — all must succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_telemetry_happy_path_after_subscription() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    // ── Step 1: configure a match-all subscription ───────────────────────────
    configure_match_all_subscription(&client, "all", "100").await;

    // ── Step 2: GetTelemetrySubscriptions (nil id → assigned) ────────────────
    let get_resp: GetTelemetrySubscriptionsResponse = client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: WireUuid::ZERO,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    check!(
        get_resp.error_code == 0,
        "GetTelemetrySubscriptions must succeed: {get_resp:?}"
    );
    check!(
        get_resp.client_instance_id != WireUuid::ZERO,
        "broker must assign a fresh client_instance_id"
    );
    check!(
        get_resp.requested_metrics == vec!["*".to_string()],
        "match-all subscription must reflect as [\"*\"], got {:?}",
        get_resp.requested_metrics,
    );
    check!(
        get_resp.push_interval_ms == 100,
        "push_interval_ms must equal the configured interval (100), got {}",
        get_resp.push_interval_ms,
    );
    check!(
        get_resp.accepted_compression_types == vec![4i8, 3, 1, 2],
        "accepted_compression_types must be [4,3,1,2], got {:?}",
        get_resp.accepted_compression_types,
    );
    check!(get_resp.delta_temporality, "delta_temporality must be true");
    check!(
        get_resp.telemetry_max_bytes == 1_048_576,
        "telemetry_max_bytes must be 1 MiB, got {}",
        get_resp.telemetry_max_bytes,
    );

    let assigned_id = get_resp.client_instance_id;
    let subscription_id = get_resp.subscription_id;

    // ── Step 3: PushTelemetry with the assigned id + subscription id ──────────
    // compression_type = 0 is NONE (uncompressed); sample_otlp_metrics()
    // returns raw (uncompressed) proto bytes, so no codec mismatch.
    let push_resp: PushTelemetryResponse = client
        .send(PushTelemetryRequest {
            client_instance_id: assigned_id,
            subscription_id,
            terminating: false,
            compression_type: 0,
            metrics: sample_otlp_metrics(),
            ..Default::default()
        })
        .await
        .expect("PushTelemetry");

    assert!(
        push_resp.error_code == 0,
        "valid push must succeed, got error_code={}",
        push_resp.error_code,
    );
}

/// A registered instance pushing with a stale subscription_id must be
/// rejected with UNKNOWN_SUBSCRIPTION_ID (117).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_telemetry_stale_subscription_id_rejected() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    configure_match_all_subscription(&client, "all", "100").await;

    let get_resp: GetTelemetrySubscriptionsResponse = client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: WireUuid::ZERO,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    assert!(get_resp.error_code == 0);
    let assigned_id = get_resp.client_instance_id;
    let real_sub_id = get_resp.subscription_id;
    // XOR with a constant to produce a definitely-wrong subscription id.
    let stale_sub_id = real_sub_id ^ 0x5555;

    let push_resp: PushTelemetryResponse = client
        .send(PushTelemetryRequest {
            client_instance_id: assigned_id,
            subscription_id: stale_sub_id,
            terminating: false,
            compression_type: 0,
            metrics: sample_otlp_metrics(),
            ..Default::default()
        })
        .await
        .expect("PushTelemetry");

    assert!(
        push_resp.error_code == 117,
        "stale subscription_id must yield UNKNOWN_SUBSCRIPTION_ID (117), got {}",
        push_resp.error_code,
    );
}

/// Unsupported compression_type must yield UNSUPPORTED_COMPRESSION_TYPE (76).
/// The manager allows the first push after a GetTelemetrySubscriptions (the
/// "first_after_get" window), so the codec check is reached.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_telemetry_unsupported_compression_rejected() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    configure_match_all_subscription(&client, "all", "100").await;

    let get_resp: GetTelemetrySubscriptionsResponse = client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: WireUuid::ZERO,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    assert!(get_resp.error_code == 0);
    let assigned_id = get_resp.client_instance_id;
    let subscription_id = get_resp.subscription_id;

    // compression_type = 5: `from_attribute_bits` masks to low 3 bits, so
    // 5 & 0b111 = 5 which maps to None → UNSUPPORTED_COMPRESSION_TYPE.
    // (Values 5,6,7 are all "reserved/unknown" in Kafka's codec table.)
    let push_resp: PushTelemetryResponse = client
        .send(PushTelemetryRequest {
            client_instance_id: assigned_id,
            subscription_id,
            terminating: false,
            compression_type: 5,
            metrics: sample_otlp_metrics(),
            ..Default::default()
        })
        .await
        .expect("PushTelemetry");

    assert!(
        push_resp.error_code == 76,
        "unsupported compression_type must yield UNSUPPORTED_COMPRESSION_TYPE (76), got {}",
        push_resp.error_code,
    );
}
