//! `DescribeAcls` handler (`api_key` 29).
//!
//! Authorizes `Describe` on `Cluster`, then projects every ACL in
//! the metadata image that matches the request's filter axes.

use bytes::Bytes;
use crabka_metadata::AclEntryFilter;
use crabka_protocol::Encode;
use crabka_protocol::owned::describe_acls_request::DescribeAclsRequest;
use crabka_protocol::owned::describe_acls_response::{
    AclDescription, DescribeAclsResource, DescribeAclsResponse,
};

use super::acl_wire::{
    operation_filter, operation_to_wire, pattern_type_filter, pattern_type_to_wire,
    permission_filter, permission_to_wire, resource_type_filter, resource_type_to_wire,
};
use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;

// `async` for symmetry with the other ACL wire handlers (CreateAcls /
// DeleteAcls awaits `controller.submit_change`; read-only
// DescribeAcls itself never suspends.
#[allow(clippy::unused_async)]
#[tracing::instrument(
    name = "handle_describe_acls",
    level = "info",
    skip_all,
    fields(api = "DescribeAcls"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeAclsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Describe,
        },
    );
    if allow == AuthorizationResult::Deny {
        let resp = DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("describe-acls denied".into()),
            resources: vec![],
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    // Decode filter axes from wire.
    let Ok(filter) = build_filter(&req) else {
        let resp = DescribeAclsResponse {
            throttle_time_ms: 0,
            error_code: codes::INVALID_REQUEST,
            error_message: Some("malformed filter axis".into()),
            resources: vec![],
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    };

    // Collect matching ACLs and group by (resource_type, resource_name,
    // pattern_type) so the wire response can mirror Kafka's nested
    // shape.
    let mut by_resource: std::collections::HashMap<(i8, String, i8), Vec<AclDescription>> =
        std::collections::HashMap::new();
    for entry in image.all_acls() {
        if !filter.matches(entry) {
            continue;
        }
        let key = (
            resource_type_to_wire(entry.resource_type),
            entry.resource_name.clone(),
            pattern_type_to_wire(entry.pattern_type),
        );
        by_resource.entry(key).or_default().push(AclDescription {
            principal: entry.principal.clone(),
            host: entry.host.clone(),
            operation: operation_to_wire(entry.operation),
            permission_type: permission_to_wire(entry.permission_type),
            ..Default::default()
        });
    }

    let resources: Vec<DescribeAclsResource> = by_resource
        .into_iter()
        .map(|((rt, rn, pt), acls)| DescribeAclsResource {
            resource_type: rt,
            resource_name: rn,
            pattern_type: pt,
            acls,
            ..Default::default()
        })
        .collect();

    let resp = DescribeAclsResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        resources,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn build_filter(
    req: &DescribeAclsRequest,
) -> Result<AclEntryFilter, super::acl_wire::WireAclError> {
    let resource_name = req
        .resource_name_filter
        .as_ref()
        .filter(|s| !s.is_empty())
        .cloned();
    let principal = req
        .principal_filter
        .as_ref()
        .filter(|s| !s.is_empty())
        .cloned();
    let host = req.host_filter.as_ref().filter(|s| !s.is_empty()).cloned();
    Ok(AclEntryFilter {
        resource_type: resource_type_filter(req.resource_type_filter)?,
        resource_name,
        pattern_type: pattern_type_filter(req.pattern_type_filter)?,
        principal,
        host,
        operation: operation_filter(req.operation)?,
        permission_type: permission_filter(req.permission_type)?,
    })
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode DescribeAcls: {e}")))?;
    Ok(Bytes::from(body))
}
