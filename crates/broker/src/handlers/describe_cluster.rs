//! `DescribeCluster` (`api_key=60`).
//!
//! This handler projects registrations from the metadata image and broker
//! fencing from the controller's heartbeat registry. It authorizes `Describe`
//! on `Cluster("kafka-cluster")`. On Deny it returns a
//! whole-response `error_code = CLUSTER_AUTHORIZATION_FAILED` (31).
//!
//! KIP-430: when the request sets the
//! `include_cluster_authorized_operations` flag, the response carries a
//! bitfield of the cluster operations the principal is authorized for. If the
//! flag is not set, the field stays at `i32::MIN`, which is Kafka's "not
//! present" sentinel.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
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

/// `DescribeCluster` `endpoint_type` (KIP-919): `1` = BROKERS.
const ENDPOINT_TYPE_BROKERS: i8 = 1;

// cargo-mutants: the only surviving mutants here flip `-1` node/controller-id
// sentinel fallbacks (`try_from(id).unwrap_or(-1)`, `watch_leader().map_or(-1, ..)`);
// broker/controller ids are int32 on the wire so `try_from` never fails, and a
// started test broker always has an elected leader, making the fallbacks
// unreachable with realistic inputs. Response shape is pinned by the tests below.
#[cfg_attr(test, mutants::skip)]
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
    if req.endpoint_type != ENDPOINT_TYPE_BROKERS {
        let resp = DescribeClusterResponse {
            error_code: codes::MISMATCHED_ENDPOINT_TYPE,
            error_message: Some("broker listener requires endpoint_type=BROKERS".into()),
            endpoint_type: req.endpoint_type,
            ..Default::default()
        };
        return crate::handlers::encode_response(&resp, version);
    }

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
        return crate::handlers::encode_response(&resp, version);
    }

    let controller_id = broker
        .controller
        .watch_leader()
        .borrow()
        .map_or(-1, |n| i32::try_from(n.0).unwrap_or(-1));

    // KIP-919: a broker listener serves only BROKERS. KIP-1073 excludes known
    // dead/fenced brokers unless the request opts in, and marks included
    // unavailable rows as fenced. Unknown liveness entries remain eligible
    // while a newly elected controller seeds its heartbeat registry.
    let is_controller = *broker.controller.watch_leader().borrow() == Some(broker.config.node_id);
    let unavailable = if is_controller {
        broker.liveness.unavailable_snapshot().await
    } else {
        std::collections::HashSet::new()
    };
    let inter_broker_name = broker.config.inter_broker_listener_name.as_str();
    let brokers: Vec<DescribeClusterBroker> = image
        .brokers()
        .filter(|b| req.include_fenced_brokers || !unavailable.contains(&b.node_id.0))
        .map(|b| {
            let (host, port) = crate::handlers::metadata::pick_endpoint_host_port(
                b,
                ctx.connection_listener_name,
                inter_broker_name,
            );
            DescribeClusterBroker {
                broker_id: i32::try_from(b.node_id.0).unwrap_or(-1),
                host,
                port,
                rack: b.rack.clone(),
                is_fenced: unavailable.contains(&b.node_id.0),
                ..Default::default()
            }
        })
        .collect();

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
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_metadata::{BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord, NodeId};
    use crabka_security::ListenerProtocol;

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 2;

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
                    node_id: NodeId(42),
                    broker_epoch: 7,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "legacy-host".into(),
                    port: 19092,
                    rack: Some("rack-a".into()),
                    log_dirs: vec![],
                    endpoints: vec![BrokerEndpoint {
                        name: "PLAINTEXT".into(),
                        host: "broker-a".into(),
                        port: 29092,
                        protocol: ListenerProtocol::Plaintext,
                    }],
                    features: std::collections::BTreeMap::new(),
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

    #[tokio::test]
    async fn fenced_brokers_require_explicit_opt_in() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_broker(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        broker.liveness.record_fenced_heartbeat(42).await;
        assert!(broker.liveness.apply_fencing(42, true, true).await);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            VERSION,
            123,
            &encode_request(&request(false)),
            &ctx,
        )
        .await
        .expect("exclude fenced broker");
        let response = decode_response(&bytes);
        assert!(response.brokers.iter().all(|row| row.broker_id != 42));

        let mut include_fenced = request(false);
        include_fenced.include_fenced_brokers = true;
        let bytes = handle(
            &broker,
            VERSION,
            123,
            &encode_request(&include_fenced),
            &ctx,
        )
        .await
        .expect("include fenced broker");
        let response = decode_response(&bytes);
        let fenced = response
            .brokers
            .iter()
            .find(|row| row.broker_id == 42)
            .expect("fenced broker row");
        assert!(fenced.is_fenced);

        broker_handle.shutdown().await;
    }
}
