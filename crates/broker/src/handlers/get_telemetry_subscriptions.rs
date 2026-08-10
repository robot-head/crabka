//! `GetTelemetrySubscriptions` (`api_key=71`, KIP-714).
//!
//! This handler assigns or echoes the client instance id. It matches the
//! client against the configured `CLIENT_METRICS` subscriptions. It then
//! returns the computed subscription, which holds the metrics, the interval,
//! and the id. See `client_metrics::manager`.

use bytes::Bytes;
use crabka_protocol::{
    Decode,
    owned::{
        get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
        get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use uuid::Uuid;

use crate::{
    broker::Broker,
    client_metrics::manager::{ACCEPTED_COMPRESSION_TYPES, ClientAttributes, SubscriptionDecision},
    codes,
    error::BrokerError,
    handlers::context::TelemetryContext,
};

#[tracing::instrument(
    name = "handle_get_telemetry_subscriptions",
    level = "info",
    skip_all,
    fields(api = "GetTelemetrySubscriptions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &TelemetryContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = GetTelemetrySubscriptionsRequest::decode(&mut cur, version)?;

    let (instance_uuid, echo_id) = if req.client_instance_id == WireUuid::ZERO {
        let fresh = Uuid::new_v4();
        (fresh, WireUuid(fresh.into_bytes()))
    } else {
        (Uuid::from_bytes(req.client_instance_id.0), WireUuid::ZERO)
    };

    let attrs = ClientAttributes {
        client_instance_id: instance_uuid,
        client_id: ctx.client_id.to_string(),
        software_name: ctx.software_name.to_string(),
        software_version: ctx.software_version.to_string(),
        source_address: ctx.peer.ip().to_string(),
        source_port: ctx.peer.port(),
    };

    let image = broker.controller.current_image();
    let resp = match broker.client_metrics.manager.assign(&image, &attrs) {
        SubscriptionDecision::Assign(assignment) => GetTelemetrySubscriptionsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            client_instance_id: echo_id,
            subscription_id: assignment.subscription_id,
            accepted_compression_types: ACCEPTED_COMPRESSION_TYPES.to_vec(),
            push_interval_ms: assignment.push_interval_ms,
            telemetry_max_bytes: broker.client_metrics.manager.telemetry_max_bytes(),
            delta_temporality: true,
            requested_metrics: assignment.metrics,
            ..Default::default()
        },
        SubscriptionDecision::Reject {
            error_code,
            throttle_ms,
        } => GetTelemetrySubscriptionsResponse {
            throttle_time_ms: throttle_ms,
            error_code,
            ..Default::default()
        },
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_protocol::owned::get_telemetry_subscriptions_response;

    use super::*;

    crate::test_support::codec_helpers!(
        GetTelemetrySubscriptionsRequest,
        GetTelemetrySubscriptionsResponse,
        version = get_telemetry_subscriptions_response::MAX_VERSION
    );

    #[tokio::test]
    async fn repeated_get_is_throttled() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|_cfg| {}).await;
        let broker = broker_handle.broker_arc_for_test();
        let peer = "127.0.0.1:9092".parse().unwrap();
        let ctx = TelemetryContext {
            client_id: "client-a",
            peer: &peer,
            software_name: "test-client",
            software_version: "1.0.0",
        };

        let first = handle(
            &broker,
            get_telemetry_subscriptions_response::MAX_VERSION,
            7,
            &encode_request(&GetTelemetrySubscriptionsRequest {
                client_instance_id: WireUuid::ZERO,
                ..Default::default()
            }),
            &ctx,
        )
        .expect("first get");
        let first = decode_response(&first);
        assert!(first.error_code == codes::NONE);
        assert!(first.client_instance_id != WireUuid::ZERO);

        let second = handle(
            &broker,
            get_telemetry_subscriptions_response::MAX_VERSION,
            8,
            &encode_request(&GetTelemetrySubscriptionsRequest {
                client_instance_id: first.client_instance_id,
                ..Default::default()
            }),
            &ctx,
        )
        .expect("second get");
        let second = decode_response(&second);
        assert!(second.error_code == codes::THROTTLING_QUOTA_EXCEEDED);
        assert!(second.throttle_time_ms == first.push_interval_ms);

        broker_handle.shutdown().await;
    }
}
