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

use bytes::Bytes;
use crabka_metadata::{MetadataRecord, UnregisterBrokerRecord};
use crabka_protocol::{
    Decode,
    owned::{
        unregister_broker_request::UnregisterBrokerRequest,
        unregister_broker_response::UnregisterBrokerResponse,
    },
};

use crate::{broker::Broker, codes, error::BrokerError, handlers::cluster_alter_denied};

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
    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        let resp = response(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("unregister-broker denied".into()),
        );
        return encode_resp(version, &resp);
    }

    // The request broker_id is signed but node ids are non-negative;
    // refuse negatives up front rather than silently `as u64`.
    if req.broker_id < 0 {
        let resp = response(
            codes::INVALID_REQUEST,
            Some(format!(
                "broker_id must be non-negative, got {}",
                req.broker_id
            )),
        );
        return encode_resp(version, &resp);
    }

    let node_id = crabka_metadata::NodeId(u64::try_from(req.broker_id).expect("non-negative"));

    // Existence check. Unknown id → INVALID_REQUEST with a clear message,
    // matching JVM's `BrokerIdNotRegisteredException → INVALID_REQUEST`
    // surface.
    if image.broker(node_id).is_none() {
        let resp = response(
            codes::INVALID_REQUEST,
            Some(format!("broker {node_id} is not registered")),
        );
        return encode_resp(version, &resp);
    }

    // Submit the unregister record through Raft. The image apply is
    // idempotent (the `apply` arm calls `brokers.remove`).
    let record = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id });
    if let Err(e) = broker.controller.submit_change(vec![record]).await {
        let resp = response(
            codes::UNKNOWN_SERVER_ERROR,
            Some(format!("controller submit failed: {e}")),
        );
        return encode_resp(version, &resp);
    }

    let resp = response(codes::NONE, None);
    encode_resp(version, &resp)
}

fn response(error_code: i16, error_message: Option<String>) -> UnregisterBrokerResponse {
    UnregisterBrokerResponse {
        error_code,
        error_message,
        ..Default::default()
    }
}

fn encode_resp(version: i16, resp: &UnregisterBrokerResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_protocol::owned::unregister_broker_response::{self, UnregisterBrokerResponse};
    use crabka_security::Principal;

    use super::*;
    use crate::{authorizer::Authorizer, broker::BrokerHandle, test_support::DenyAll};

    fn encode_request(req: &UnregisterBrokerRequest, version: i16) -> Bytes {
        crate::test_support::encode_request(req, version)
    }

    fn decode_response(bytes: &Bytes) -> UnregisterBrokerResponse {
        crate::test_support::decode_response(bytes, unregister_broker_response::MAX_VERSION)
    }

    fn principal() -> Principal {
        crate::test_support::principal("admin")
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "unregister-client")
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = authorizer;
        })
        .await
    }

    #[test]
    fn response_preserves_error_fields_and_throttle() {
        let resp = response(codes::UNKNOWN_SERVER_ERROR, Some("submit failed".into()));

        let expected = UnregisterBrokerResponse {
            throttle_time_ms: 0,
            error_code: codes::UNKNOWN_SERVER_ERROR,
            error_message: Some("submit failed".into()),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_with_message_and_throttle() {
        let version = unregister_broker_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = UnregisterBrokerRequest {
            broker_id: 1,
            ..Default::default()
        };

        let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = UnregisterBrokerResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("unregister-broker denied".into()),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_negative_broker_id_before_casting() {
        let version = unregister_broker_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = UnregisterBrokerRequest {
            broker_id: -1,
            ..Default::default()
        };

        let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = UnregisterBrokerResponse {
            throttle_time_ms: 0,
            error_code: codes::INVALID_REQUEST,
            error_message: Some("broker_id must be non-negative, got -1".into()),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_treats_zero_as_non_negative_unknown_broker() {
        let version = unregister_broker_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = UnregisterBrokerRequest {
            broker_id: 0,
            ..Default::default()
        };

        let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = UnregisterBrokerResponse {
            throttle_time_ms: 0,
            error_code: codes::INVALID_REQUEST,
            error_message: Some("broker 0 is not registered".into()),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unregisters_registered_broker_with_success_shape() {
        let version = unregister_broker_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = UnregisterBrokerRequest {
            broker_id: 1,
            ..Default::default()
        };

        let resp = handle(&broker, version, 1, &encode_request(&req, version), &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = UnregisterBrokerResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }
}
