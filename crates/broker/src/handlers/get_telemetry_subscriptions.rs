//! `GetTelemetrySubscriptions` (`api_key=71`, KIP-714). Clients call this
//! to discover whether the broker wants OTel-encoded client metrics
//! pushed to it.
//!
//! Crabka exposes its own broker-side observability via Prometheus
//! (`crabka_broker_*` metrics on `/metrics`) and OTLP tracing (slice 42)
//! — it has no use for ingesting client-side metrics. The minimal-viable
//! KIP-714 handshake therefore tells every client "no metrics
//! subscribed" via an empty `requested_metrics` array. Per KIP-714, the
//! JVM client treats an empty subscription as "don't push" and skips
//! the follow-up [`PushTelemetryRequest`] traffic entirely.
//!
//! The handler still honors the KIP-714 client-instance-id assignment
//! contract:
//!
//! - Request `client_instance_id == nil`: assign a fresh v4 UUID and
//!   echo it in the response (clients cache it for subsequent calls).
//! - Request `client_instance_id != nil`: echo `nil` in the response
//!   per the upstream schema (which says "Assigned client instance id
//!   if `ClientInstanceId` was 0 in the request, else 0").

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use uuid::Uuid;

use crabka_protocol::owned::get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest;
use crabka_protocol::owned::get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

/// Sentinel "no telemetry subscription" lifetime in milliseconds — five
/// minutes, matching the JVM `client.telemetry.push.interval.ms`
/// default. Affects only the client's wake-up cadence to re-fetch the
/// subscription; emit any reasonable value.
const NO_SUBSCRIPTION_PUSH_INTERVAL_MS: i32 = 300_000;

pub(crate) fn handle(
    _broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = GetTelemetrySubscriptionsRequest::decode(&mut cur, version)?;

        // Client sent `nil` → assign a fresh v4 UUID. Otherwise → echo
        // `nil` back (the schema mandates this asymmetric "return id
        // only on first assignment" shape).
        let assigned_id = if req.client_instance_id == WireUuid::ZERO {
            WireUuid(Uuid::new_v4().into_bytes())
        } else {
            WireUuid::ZERO
        };

        let resp = GetTelemetrySubscriptionsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            client_instance_id: assigned_id,
            // No tracked subscription → any subscription_id is fine.
            // Use 0 (the schema-level default) so PushTelemetry
            // requests carrying `subscription_id = 0` won't trip the
            // (future) UNKNOWN_SUBSCRIPTION_ID gate.
            subscription_id: 0,
            // Compression doesn't matter — we accept nothing — but
            // emitting the empty list mirrors a real broker that has
            // no subscriptions configured.
            accepted_compression_types: Vec::new(),
            push_interval_ms: NO_SUBSCRIPTION_PUSH_INTERVAL_MS,
            telemetry_max_bytes: 0,
            delta_temporality: false,
            // KIP-714: an empty `requested_metrics` array signals "no
            // metrics subscribed". Clients skip PushTelemetry entirely.
            requested_metrics: Vec::new(),
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
