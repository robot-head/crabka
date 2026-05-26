//! `DescribeCluster` (`api_key=60`). Pure projection over the metadata
//! image. Authorizes `Describe` on `Cluster("kafka-cluster")`; on Deny
//! returns a whole-response `error_code = CLUSTER_AUTHORIZATION_FAILED` (31).
//! The `cluster_authorized_operations` field is set to `i32::MIN`
//! (Apache Kafka's "not present" sentinel).

use bytes::{Bytes, BytesMut};

use crabka_metadata::AclOperation;
use crabka_protocol::Encode;
use crabka_protocol::owned::describe_cluster_response::{
    DescribeClusterBroker, DescribeClusterResponse,
};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

// `async` for symmetry with other handlers that do await `controller.submit_change`;
// DescribeCluster is read-only so it never suspends.
#[allow(clippy::unused_async)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    _req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let image = broker.controller.current_image();

    // ── slice-13 ACL preamble ────────────────────────────────────────
    // Whole-request Cluster Describe gate. On Deny, return
    // CLUSTER_AUTHORIZATION_FAILED on the whole response.
    let allow = broker.config.authorizer.authorize(
        &image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: "kafka-cluster",
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

    let brokers: Vec<DescribeClusterBroker> = image
        .brokers()
        .map(|b| DescribeClusterBroker {
            broker_id: i32::try_from(b.node_id).unwrap_or(-1),
            host: b.host.clone(),
            port: i32::from(b.port),
            rack: b.rack.clone(),
            ..Default::default()
        })
        .collect();

    let resp = DescribeClusterResponse {
        error_code: codes::NONE,
        error_message: None,
        cluster_id: image.cluster_id().to_string(),
        controller_id,
        brokers,
        cluster_authorized_operations: i32::MIN,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
