// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.
#![allow(clippy::pedantic)]

//! KIP-714 client metrics push handshake. Crabka exposes its own
//! Prometheus broker-side observability and doesn't consume client
//! metrics, so the broker's job here is to advertise the API key range
//! on `ApiVersions` and then say "no metrics subscribed" on every
//! `GetTelemetrySubscriptions` call. Well-behaved JVM clients skip
//! `PushTelemetry` entirely once they see the empty subscription.
//!
//! Tests:
//!   * `ApiVersions` advertises api_key 71 (`GetTelemetrySubscriptions`)
//!     and api_key 72 (`PushTelemetry`).
//!   * `GetTelemetrySubscriptions { client_instance_id = nil }` →
//!     response carries a freshly-assigned, non-nil `client_instance_id`
//!     and `requested_metrics` is empty (the "don't push" signal).
//!   * `GetTelemetrySubscriptions { client_instance_id = <set> }` →
//!     response echoes `nil` for `client_instance_id` per the upstream
//!     schema convention.
//!   * `PushTelemetry` with arbitrary payload returns `error_code = 0`
//!     (defensive no-op for clients racing the subscription re-fetch).

#![cfg(not(target_os = "windows"))]

mod support;

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest;
use crabka_protocol::owned::push_telemetry_request::PushTelemetryRequest;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

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

#[tokio::test]
async fn get_telemetry_subscriptions_with_nil_id_returns_assigned_id_and_empty_subscription() {
    let p = support::start().await;

    let resp = p
        .client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: WireUuid::ZERO,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    assert_eq!(resp.error_code, 0, "no-op handler must succeed: {resp:?}");
    assert_ne!(
        resp.client_instance_id,
        WireUuid::ZERO,
        "broker must assign a fresh client_instance_id when caller sent nil",
    );
    // The crucial KIP-714 signal: empty `requested_metrics` → JVM
    // client treats this as "no subscription" and skips PushTelemetry.
    assert!(
        resp.requested_metrics.is_empty(),
        "no-op handler must advertise no subscription, got {:?}",
        resp.requested_metrics,
    );
    // `accepted_compression_types` empty + `telemetry_max_bytes` 0
    // belt-and-braces the "don't push" signal.
    assert!(resp.accepted_compression_types.is_empty());
    assert_eq!(resp.telemetry_max_bytes, 0);
    // The push interval shouldn't be so low that clients spin on
    // re-fetching — 5 minutes is the JVM default.
    assert!(
        resp.push_interval_ms >= 60_000,
        "push_interval_ms too low: {}",
        resp.push_interval_ms,
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn get_telemetry_subscriptions_with_set_id_echoes_nil() {
    let p = support::start().await;

    // Caller has already been assigned an id from a prior call (or
    // generated their own) — broker must echo `nil` per the upstream
    // schema convention: "Assigned client instance id if
    // ClientInstanceId was 0 in the request, else 0."
    let prior_id = WireUuid([0x11; 16]);
    let resp = p
        .client
        .send(GetTelemetrySubscriptionsRequest {
            client_instance_id: prior_id,
            ..Default::default()
        })
        .await
        .expect("GetTelemetrySubscriptions");

    assert_eq!(resp.error_code, 0);
    assert_eq!(
        resp.client_instance_id,
        WireUuid::ZERO,
        "non-nil request id must round-trip as nil per schema rules",
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn push_telemetry_accepts_no_op_with_arbitrary_payload() {
    let p = support::start().await;

    // Send a push the broker isn't expecting (an empty subscription
    // means the client shouldn't push, but a race-window client might).
    // The no-op handler must still ack so the client doesn't retry.
    let resp = p
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

    assert_eq!(resp.error_code, 0, "no-op handler must ack: {resp:?}");

    p.broker.shutdown().await;
}
