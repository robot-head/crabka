//! `UnregisterBroker` (`api_key=64`). Admin RPC the operator uses to
//! drop a permanently-dead broker from the cluster's metadata image.
//! After this lands through Raft, `Metadata` responses no longer
//! advertise the broker's endpoints; clients stop routing to it.
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! ## Idempotency
//!
//! Unknown `broker_id` returns `INVALID_REQUEST (42)` with an
//! explanatory message — matches JVM `KafkaApis.handleUnregisterBroker`
//! shape (it surfaces `BrokerIdNotRegisteredException` as
//! `INVALID_REQUEST`).

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, MetadataRecord, ResourceType, UnregisterBrokerRecord};
use crabka_protocol::owned::unregister_broker_request::UnregisterBrokerRequest;
use crabka_protocol::owned::unregister_broker_response::UnregisterBrokerResponse;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_unregister_broker",
    level = "info",
    skip_all,
    fields(api = "UnregisterBroker", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = UnregisterBrokerRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Cluster:Alter gate.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: AclOperation::Alter,
        },
    );
    if allow == AuthorizationResult::Deny {
        let resp = UnregisterBrokerResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("unregister-broker denied".into()),
            throttle_time_ms: 0,
            ..Default::default()
        };
        return encode_resp(version, &resp);
    }

    // The request broker_id is signed but node ids are non-negative;
    // refuse negatives up front rather than silently `as u64`.
    if req.broker_id < 0 {
        let resp = UnregisterBrokerResponse {
            error_code: codes::INVALID_REQUEST,
            error_message: Some(format!(
                "broker_id must be non-negative, got {}",
                req.broker_id
            )),
            throttle_time_ms: 0,
            ..Default::default()
        };
        return encode_resp(version, &resp);
    }

    let node_id = u64::try_from(req.broker_id).expect("non-negative");

    // Existence check. Unknown id → INVALID_REQUEST with a clear message,
    // matching JVM's `BrokerIdNotRegisteredException → INVALID_REQUEST`
    // surface.
    if image.broker(node_id).is_none() {
        let resp = UnregisterBrokerResponse {
            error_code: codes::INVALID_REQUEST,
            error_message: Some(format!("broker {node_id} is not registered")),
            throttle_time_ms: 0,
            ..Default::default()
        };
        return encode_resp(version, &resp);
    }

    // Submit the unregister record through Raft. The image apply is
    // idempotent (the `apply` arm calls `brokers.remove`).
    let record = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id });
    if let Err(e) = broker.controller.submit_change(vec![record]).await {
        let resp = UnregisterBrokerResponse {
            error_code: codes::UNKNOWN_SERVER_ERROR,
            error_message: Some(format!("controller submit failed: {e}")),
            throttle_time_ms: 0,
            ..Default::default()
        };
        return encode_resp(version, &resp);
    }

    let resp = UnregisterBrokerResponse {
        error_code: codes::NONE,
        error_message: None,
        throttle_time_ms: 0,
        ..Default::default()
    };
    encode_resp(version, &resp)
}

fn encode_resp(version: i16, resp: &UnregisterBrokerResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
