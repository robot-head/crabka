//! `DescribeAcls` handler (`api_key` 29).
//!
//! Authorizes `Describe` on `Cluster`, then projects every ACL in
//! the metadata image that matches the request's filter axes.

use bytes::Bytes;
use crabka_metadata::{AclEntry, AclEntryFilter};
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

fn describe_acls_error_response(
    error_code: i16,
    error_message: &'static str,
) -> DescribeAclsResponse {
    DescribeAclsResponse {
        error_code,
        error_message: Some(error_message.into()),
        ..Default::default()
    }
}

fn acl_description(entry: &AclEntry) -> AclDescription {
    AclDescription {
        principal: entry.principal.clone(),
        host: entry.host.clone(),
        operation: operation_to_wire(entry.operation),
        permission_type: permission_to_wire(entry.permission_type),
        ..Default::default()
    }
}

fn describe_acls_resource(
    resource_type: i8,
    resource_name: String,
    pattern_type: i8,
    acls: Vec<AclDescription>,
) -> DescribeAclsResource {
    DescribeAclsResource {
        resource_type,
        resource_name,
        pattern_type,
        acls,
        ..Default::default()
    }
}

fn describe_acls_response(resources: Vec<DescribeAclsResource>) -> DescribeAclsResponse {
    DescribeAclsResponse {
        resources,
        ..Default::default()
    }
}

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
        let resp = describe_acls_error_response(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "describe-acls denied",
        );
        return encode_response(&resp, api_version);
    }

    // Decode filter axes from wire.
    let Ok(filter) = build_filter(&req) else {
        let resp = describe_acls_error_response(codes::INVALID_REQUEST, "malformed filter axis");
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
        by_resource
            .entry(key)
            .or_default()
            .push(acl_description(entry));
    }

    let resources: Vec<DescribeAclsResource> = by_resource
        .into_iter()
        .map(|((rt, rn, pt), acls)| describe_acls_resource(rt, rn, pt, acls))
        .collect();

    let resp = describe_acls_response(resources);
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{
        AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
    };
    use crabka_protocol::Decode;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, Authorizer};
    use crate::broker::{Broker, BrokerHandle};
    use crate::config::BrokerConfig;

    const VERSION: i16 = 3;
    const RESOURCE_TYPE_TOPIC: i8 = 2;
    const PATTERN_TYPE_ANY: i8 = 1;
    const PATTERN_TYPE_LITERAL: i8 = 3;
    const OPERATION_ANY: i8 = 1;
    const OPERATION_READ: i8 = 3;
    const OPERATION_WRITE: i8 = 4;
    const PERMISSION_ANY: i8 = 1;
    const PERMISSION_ALLOW: i8 = 3;

    #[derive(Debug)]
    struct DenyAll;

    impl Authorizer for DenyAll {
        fn authorize(
            &self,
            _source: &dyn crabka_authz::AclSource,
            _req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Deny
        }
    }

    fn acl(resource_name: &str, principal: &str, operation: AclOperation) -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: resource_name.into(),
            pattern_type: PatternType::Literal,
            principal: principal.into(),
            host: "*".into(),
            operation,
            permission_type: PermissionType::Allow,
        }
    }

    fn request(
        resource_name: Option<&str>,
        principal: Option<&str>,
        operation: i8,
    ) -> DescribeAclsRequest {
        DescribeAclsRequest {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: resource_name.map(Into::into),
            pattern_type_filter: PATTERN_TYPE_LITERAL,
            principal_filter: principal.map(Into::into),
            host_filter: Some("*".into()),
            operation,
            permission_type: PERMISSION_ALLOW,
            ..Default::default()
        }
    }

    fn decode_response(bytes: Bytes) -> DescribeAclsResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = DescribeAclsResponse::decode(&mut cur, VERSION).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "admin-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn peer() -> SocketAddr {
        "127.0.0.1:9092".parse().unwrap()
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    async fn seed_acls(handle: &BrokerHandle, entries: Vec<AclEntry>) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(
                entries
                    .into_iter()
                    .map(MetadataRecord::V1AccessControlEntry)
                    .collect(),
            )
            .await
            .expect("seed ACLs");
    }

    #[test]
    fn build_filter_collapses_empty_strings_and_decodes_axes() {
        let req = DescribeAclsRequest {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some(String::new()),
            pattern_type_filter: PATTERN_TYPE_ANY,
            principal_filter: Some(String::new()),
            host_filter: Some(String::new()),
            operation: OPERATION_ANY,
            permission_type: PERMISSION_ANY,
            ..Default::default()
        };

        let built = build_filter(&req).expect("filter");

        assert!(built.resource_type == Some(ResourceType::Topic));
        assert!(built.resource_name.is_none());
        assert!(built.pattern_type.is_none());
        assert!(built.principal.is_none());
        assert!(built.host.is_none());
        assert!(built.operation.is_none());
        assert!(built.permission_type.is_none());
    }

    #[test]
    fn build_filter_rejects_malformed_axes() {
        let mut req = request(Some("orders"), Some("User:alice"), OPERATION_READ);
        req.resource_type_filter = 99;
        assert!(build_filter(&req).is_err());

        let mut req = request(Some("orders"), Some("User:alice"), OPERATION_READ);
        req.pattern_type_filter = 99;
        assert!(build_filter(&req).is_err());

        let mut req = request(Some("orders"), Some("User:alice"), OPERATION_READ);
        req.operation = 99;
        assert!(build_filter(&req).is_err());
    }

    #[test]
    fn response_helpers_preserve_error_resource_and_acl_fields() {
        let err = describe_acls_error_response(codes::INVALID_REQUEST, "malformed filter axis");
        assert!(err.throttle_time_ms == 0);
        assert!(err.error_code == codes::INVALID_REQUEST);
        assert!(err.error_message.as_deref() == Some("malformed filter axis"));
        assert!(err.resources.is_empty());

        let desc = acl_description(&acl("orders", "User:alice", AclOperation::Read));
        assert!(desc.principal == "User:alice");
        assert!(desc.host == "*");
        assert!(desc.operation == OPERATION_READ);
        assert!(desc.permission_type == PERMISSION_ALLOW);

        let resource = describe_acls_resource(
            RESOURCE_TYPE_TOPIC,
            "orders".into(),
            PATTERN_TYPE_LITERAL,
            vec![desc],
        );
        assert!(resource.resource_type == RESOURCE_TYPE_TOPIC);
        assert!(resource.resource_name == "orders");
        assert!(resource.pattern_type == PATTERN_TYPE_LITERAL);
        assert!(resource.acls.len() == 1);

        let resp = describe_acls_response(vec![resource]);
        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::NONE);
        assert!(resp.error_message.is_none());
        assert!(resp.resources.len() == 1);
    }

    #[tokio::test]
    async fn handle_denies_cluster_describe() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let resp = handle(
            &broker,
            request(Some("orders"), Some("User:alice"), OPERATION_READ),
            &ctx,
            VERSION,
        )
        .await
        .expect("handle");
        let resp = decode_response(resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        assert!(resp.error_message.as_deref() == Some("describe-acls denied"));
        assert!(resp.resources.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_malformed_filter() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let mut req = request(Some("orders"), Some("User:alice"), OPERATION_READ);
        req.resource_type_filter = 99;

        let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
        let resp = decode_response(resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::INVALID_REQUEST);
        assert!(resp.error_message.as_deref() == Some("malformed filter axis"));
        assert!(resp.resources.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_returns_only_matching_acl_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_acls(
            &broker_handle,
            vec![
                acl("orders", "User:alice", AclOperation::Read),
                acl("payments", "User:bob", AclOperation::Write),
            ],
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let resp = handle(
            &broker,
            request(Some("orders"), Some("User:alice"), OPERATION_READ),
            &ctx,
            VERSION,
        )
        .await
        .expect("handle");
        let resp = decode_response(resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.error_code == codes::NONE);
        assert!(resp.error_message.is_none());
        assert!(resp.resources.len() == 1);
        let resource = &resp.resources[0];
        assert!(resource.resource_type == RESOURCE_TYPE_TOPIC);
        assert!(resource.resource_name == "orders");
        assert!(resource.pattern_type == PATTERN_TYPE_LITERAL);
        assert!(resource.acls.len() == 1);
        let acl = &resource.acls[0];
        assert!(acl.principal == "User:alice");
        assert!(acl.host == "*");
        assert!(acl.operation == OPERATION_READ);
        assert!(acl.permission_type == PERMISSION_ALLOW);
        broker_handle.shutdown().await;
    }

    #[test]
    fn acl_description_preserves_non_read_operations() {
        let desc = acl_description(&acl("payments", "User:bob", AclOperation::Write));

        assert!(desc.principal == "User:bob");
        assert!(desc.operation == OPERATION_WRITE);
    }
}
