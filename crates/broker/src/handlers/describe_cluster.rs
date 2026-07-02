//! `DescribeCluster` (`api_key=60`). Pure projection over the metadata
//! image. Authorizes `Describe` on `Cluster("kafka-cluster")`; on Deny
//! returns a whole-response `error_code = CLUSTER_AUTHORIZATION_FAILED` (31).
//!
//! KIP-430: when the request's `include_cluster_authorized_operations`
//! flag is set, the response carries a bitfield of the cluster
//! operations the principal is authorized for; otherwise the field is
//! left at `i32::MIN` (Kafka's "not present" sentinel).

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{
        acl_wire::CLUSTER_RESOURCE_NAME, authorized_operations::authorized_operations_bits,
    },
};

/// `DescribeCluster` `endpoint_type` (KIP-919): `1` = BROKERS (default),
/// `2` = CONTROLLERS.
const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;

// `async` for symmetry with other handlers that do await `controller.submit_change`;
// DescribeCluster is read-only so it never suspends.
#[allow(clippy::unused_async)]
#[tracing::instrument(
    name = "handle_describe_cluster",
    level = "info",
    skip_all,
    fields(api = "DescribeCluster", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: crate::handlers::ApiVersion,
    _correlation_id: crate::handlers::CorrelationId,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let image = broker.controller.current_image();

    let mut cur: &[u8] = req_bytes;
    let req = DescribeClusterRequest::decode(&mut cur, version)?;

    // ── ACL preamble ────────────────────────────────────────
    // Whole-request Cluster Describe gate. On Deny, return
    // CLUSTER_AUTHORIZATION_FAILED on the whole response.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Describe,
        },
    );
    if allow == AuthorizationResult::Deny {
        let resp = DescribeClusterResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("describe-cluster denied".into()),
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        return Ok(buf.freeze());
    }

    let controller_id = broker
        .controller
        .watch_leader()
        .borrow()
        .map_or(-1, |n| i32::try_from(n).unwrap_or(-1));

    // KIP-919: the request's `endpoint_type` selects which node set to
    // advertise — `1` (BROKERS, the default) or `2` (CONTROLLERS). For
    // CONTROLLERS we project the KRaft voter set's controller endpoints so an
    // AdminClient can discover the controller quorum (and, via
    // `--bootstrap-controller`, dial it directly). `endpoint_type` is a v1+
    // field; on v0 it defaults to `1`, so the BROKERS branch is taken.
    let brokers: Vec<DescribeClusterBroker> = if req.endpoint_type == ENDPOINT_TYPE_CONTROLLERS {
        image
            .voters()
            .iter()
            .map(|v| {
                // Prefer the voter's CONTROLLER-named listener endpoint; fall
                // back to its first advertised endpoint.
                let ep = v
                    .endpoints
                    .iter()
                    .find(|e| e.name.eq_ignore_ascii_case("CONTROLLER"))
                    .or_else(|| v.endpoints.first());
                DescribeClusterBroker {
                    broker_id: i32::try_from(v.id).unwrap_or(-1),
                    host: ep.map(|e| e.host.clone()).unwrap_or_default(),
                    port: ep.map_or(-1, |e| i32::from(e.port)),
                    rack: None,
                    ..Default::default()
                }
            })
            .collect()
    } else {
        // BROKERS (default): advertise each broker's address for the listener
        // this request arrived on (Kafka returns the connection listener's
        // advertised address), with the same fallback chain as `Metadata` so
        // the two RPCs agree.
        let inter_broker_name = broker.config.inter_broker_listener_name.as_str();
        image
            .brokers()
            .map(|b| {
                let (host, port) = crate::handlers::metadata::pick_endpoint_host_port(
                    b,
                    ctx.connection_listener_name,
                    inter_broker_name,
                );
                DescribeClusterBroker {
                    broker_id: i32::try_from(b.node_id).unwrap_or(-1),
                    host,
                    port,
                    rack: b.rack.clone(),
                    ..Default::default()
                }
            })
            .collect()
    };

    // KIP-430: only populate the bitfield when the client asked for it;
    // otherwise leave the wire-default `i32::MIN` ("not present") sentinel.
    let cluster_authorized_operations = if req.include_cluster_authorized_operations {
        authorized_operations_bits(
            broker.config.authorizer.as_ref(),
            &image,
            ctx.principal,
            ctx.peer,
            ResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
        )
    } else {
        i32::MIN
    };

    let resp = DescribeClusterResponse {
        error_code: codes::NONE,
        error_message: None,
        // Echo the requested endpoint type (KIP-919). v0 has no such field; the
        // request default of `1` keeps the response byte-identical there.
        endpoint_type: req.endpoint_type,
        cluster_id: image.cluster_id().to_string(),
        controller_id,
        brokers,
        cluster_authorized_operations,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_metadata::{BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord};
    use crabka_security::ListenerProtocol;

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 1;

    crate::test_support::wire_helpers!(
        DescribeClusterRequest,
        DescribeClusterResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    fn request(include_ops: bool) -> DescribeClusterRequest {
        DescribeClusterRequest {
            include_cluster_authorized_operations: include_ops,
            endpoint_type: 1,
            ..Default::default()
        }
    }

    async fn seed_broker(handle: &BrokerHandle) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: 42,
                    broker_epoch: 7,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "legacy-host".into(),
                    port: 19092,
                    rack: Some("rack-a".into()),
                    endpoints: vec![BrokerEndpoint {
                        name: "PLAINTEXT".into(),
                        host: "broker-a".into(),
                        port: 29092,
                        protocol: ListenerProtocol::Plaintext,
                    }],
                },
            )])
            .await
            .expect("seed broker registration");
    }

    #[tokio::test]
    async fn denied_response_preserves_error_fields() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = encode_request(&request(false));

        let bytes = handle(&broker, VERSION, 123, &req, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&bytes);

        let expected = DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("describe-cluster denied".into()),
            endpoint_type: 1,
            cluster_id: String::new(),
            controller_id: -1,
            brokers: vec![],
            cluster_authorized_operations: i32::MIN,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn broker_endpoint_response_preserves_non_default_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_broker(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = encode_request(&request(false));

        let bytes = handle(&broker, VERSION, 123, &req, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&bytes);

        assert!(
            (
                resp.error_code,
                resp.error_message.clone(),
                resp.endpoint_type,
                resp.cluster_id.clone(),
                resp.cluster_authorized_operations,
                resp.throttle_time_ms
            ) == (
                codes::NONE,
                None,
                1,
                broker.controller.current_image().cluster_id().to_string(),
                i32::MIN,
                0
            )
        );
        let broker_row = resp
            .brokers
            .iter()
            .find(|b| b.broker_id == 42)
            .expect("seeded broker row");
        let expected_row = DescribeClusterBroker {
            broker_id: 42,
            host: "broker-a".into(),
            port: 29092,
            rack: Some("rack-a".into()),
            is_fenced: false,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(*broker_row == expected_row);
        broker_handle.shutdown().await;
    }
}
