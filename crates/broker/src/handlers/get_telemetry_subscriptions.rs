//! `GetTelemetrySubscriptions` (`api_key=71`, KIP-714). Assigns/echoes the
//! client instance id, matches the client against configured `CLIENT_METRICS`
//! subscriptions, and returns the computed subscription (metrics, interval,
//! id). See `client_metrics::manager`.

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
    client_metrics::manager::{ACCEPTED_COMPRESSION_TYPES, ClientAttributes},
    codes,
    error::BrokerError,
    handlers::context::TelemetryContext,
};

#[allow(clippy::unused_async)] // signature symmetry with other inline-intercept handlers
#[tracing::instrument(
    name = "handle_get_telemetry_subscriptions",
    level = "info",
    skip_all,
    fields(api = "GetTelemetrySubscriptions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
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
    let assignment = broker.client_metrics.manager.assign(&image, &attrs);

    let resp = GetTelemetrySubscriptionsResponse {
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
    };
    crate::handlers::encode_response(&resp, version)
}
