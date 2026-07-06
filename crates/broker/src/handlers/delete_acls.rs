//! `DeleteAcls` handler (`api_key` 31).

use bytes::Bytes;
use crabka_metadata::{AclEntry, AclEntryFilter, MetadataRecord};
use crabka_protocol::{
    Encode,
    owned::{
        delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
        delete_acls_response::{DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse},
    },
};

use super::acl_wire::{
    CLUSTER_RESOURCE_NAME, operation_filter, operation_to_wire, pattern_type_filter,
    pattern_type_to_wire, permission_filter, permission_to_wire, resource_type_filter,
    resource_type_to_wire,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_delete_acls",
    level = "info",
    skip_all,
    fields(api = "DeleteAcls"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DeleteAclsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();

    // Whole-request cluster-alter gate.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if allow == AuthorizationResult::Deny {
        let filter_results = req
            .filters
            .iter()
            .map(|_| {
                filter_result(
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("delete-acls denied".into()),
                    Vec::new(),
                )
            })
            .collect();
        return encode_response(&delete_acls_response(filter_results), api_version);
    }

    let mut filter_results: Vec<DeleteAclsFilterResult> = Vec::with_capacity(req.filters.len());
    let mut to_submit: Vec<MetadataRecord> = Vec::with_capacity(req.filters.len());

    for f in &req.filters {
        match build_filter(f) {
            Ok(filter) => {
                let matched: Vec<&AclEntry> =
                    image.all_acls().filter(|e| filter.matches(e)).collect();
                let matching_acls = matched
                    .iter()
                    .map(|e| matching_acl_result(e))
                    .collect::<Vec<_>>();
                filter_results.push(filter_result(codes::NONE, None, matching_acls));
                to_submit.push(MetadataRecord::V1DeleteAccessControlEntry(filter));
            }
            Err(_) => {
                filter_results.push(filter_result(
                    codes::INVALID_REQUEST,
                    Some("malformed filter axis".into()),
                    Vec::new(),
                ));
            }
        }
    }

    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "delete-acls submit failed");
        apply_submit_error(&mut filter_results, &e);
    }

    // Audit: emit one AdminOperation record for successfully-deleted ACLs.
    // Collect resource_name from each matching ACL in every filter result that
    // committed without error (error_code == 0).
    audit_deleted_acls(
        broker.audit_log.as_ref(),
        ctx,
        deleted_acl_resources(&filter_results),
    );

    encode_response(&delete_acls_response(filter_results), api_version)
}

fn matching_acl_result(e: &AclEntry) -> DeleteAclsMatchingAcl {
    DeleteAclsMatchingAcl {
        resource_type: resource_type_to_wire(e.resource_type),
        resource_name: e.resource_name.clone(),
        pattern_type: pattern_type_to_wire(e.pattern_type),
        principal: e.principal.clone(),
        host: e.host.clone(),
        operation: operation_to_wire(e.operation),
        permission_type: permission_to_wire(e.permission_type),
        ..Default::default()
    }
}

fn filter_result(
    error_code: i16,
    error_message: Option<String>,
    matching_acls: Vec<DeleteAclsMatchingAcl>,
) -> DeleteAclsFilterResult {
    DeleteAclsFilterResult {
        error_code,
        error_message,
        matching_acls,
        ..Default::default()
    }
}

fn delete_acls_response(filter_results: Vec<DeleteAclsFilterResult>) -> DeleteAclsResponse {
    DeleteAclsResponse {
        filter_results,
        ..Default::default()
    }
}

fn apply_submit_error<E: std::fmt::Display>(filter_results: &mut [DeleteAclsFilterResult], err: E) {
    let msg = format!("submit failed: {err}");
    for r in filter_results {
        if r.error_code == codes::NONE {
            r.error_code = codes::COORDINATOR_NOT_AVAILABLE;
            r.error_message = Some(msg.clone());
        }
    }
}

fn deleted_acl_resources(
    filter_results: &[DeleteAclsFilterResult],
) -> Vec<crabka_audit::AuditResource> {
    filter_results
        .iter()
        .filter(|r| r.error_code == codes::NONE)
        .flat_map(|r| r.matching_acls.iter())
        .map(|m| crabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: m.resource_name.clone(),
        })
        .collect()
}

fn audit_deleted_acls(
    audit_log: &crabka_audit::AuditLog,
    ctx: &crate::handlers::RequestContext<'_>,
    deleted_acls: Vec<crabka_audit::AuditResource>,
) {
    if !deleted_acls.is_empty() {
        crate::handlers::audit_admin(
            audit_log,
            ctx,
            "DeleteAcls",
            crabka_audit::AuditOutcome::Success,
            deleted_acls,
        );
    }
}

fn build_filter(f: &DeleteAclsFilter) -> Result<AclEntryFilter, super::acl_wire::WireAclError> {
    let resource_name = f.resource_name_filter.clone().filter(|s| !s.is_empty());
    let principal = f.principal_filter.clone().filter(|s| !s.is_empty());
    let host = f.host_filter.clone().filter(|s| !s.is_empty());
    Ok(AclEntryFilter {
        resource_type: resource_type_filter(f.resource_type_filter)?,
        resource_name,
        pattern_type: pattern_type_filter(f.pattern_type_filter)?,
        principal,
        host,
        operation: operation_filter(f.operation)?,
        permission_type: permission_filter(f.permission_type)?,
    })
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode DeleteAcls")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};
    use crabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 3;
    const RESOURCE_TYPE_TOPIC: i8 = 2;
    const PATTERN_TYPE_ANY: i8 = 1;
    const PATTERN_TYPE_LITERAL: i8 = 3;
    const PATTERN_TYPE_PREFIXED: i8 = 4;
    const OPERATION_ANY: i8 = 1;
    const OPERATION_READ: i8 = 3;
    const OPERATION_WRITE: i8 = 4;
    const PERMISSION_ANY: i8 = 1;
    const PERMISSION_ALLOW: i8 = 3;

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

    fn filter(resource_name: Option<&str>, principal: Option<&str>) -> DeleteAclsFilter {
        DeleteAclsFilter {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: resource_name.map(Into::into),
            pattern_type_filter: PATTERN_TYPE_LITERAL,
            principal_filter: principal.map(Into::into),
            host_filter: Some("*".into()),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
            ..Default::default()
        }
    }

    fn request(filters: Vec<DeleteAclsFilter>) -> DeleteAclsRequest {
        DeleteAclsRequest {
            filters,
            ..Default::default()
        }
    }

    crate::test_support::response_helpers!(
        DeleteAclsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

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

    fn all_acls(handle: &BrokerHandle) -> Vec<AclEntry> {
        handle
            .controller_image_for_test()
            .all_acls()
            .cloned()
            .collect()
    }

    #[test]
    fn build_filter_collapses_empty_strings_and_decodes_axes() {
        let f = DeleteAclsFilter {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some(String::new()),
            pattern_type_filter: PATTERN_TYPE_ANY,
            principal_filter: Some(String::new()),
            host_filter: Some(String::new()),
            operation: OPERATION_ANY,
            permission_type: PERMISSION_ANY,
            ..Default::default()
        };

        let built = build_filter(&f).expect("filter");

        let expected = AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            resource_name: None,
            pattern_type: None,
            principal: None,
            host: None,
            operation: None,
            permission_type: None,
        };
        assert!(built == expected);
    }

    #[test]
    fn build_filter_rejects_malformed_axes() {
        #[allow(clippy::type_complexity)]
        let cases: [(&str, fn(&mut DeleteAclsFilter)); 3] = [
            ("resource_type_filter", |f| f.resource_type_filter = 99),
            ("pattern_type_filter", |f| f.pattern_type_filter = 99),
            ("operation", |f| f.operation = 99),
        ];
        for (axis, corrupt) in cases {
            let mut f = filter(Some("orders"), Some("User:alice"));
            corrupt(&mut f);
            assert!(build_filter(&f).is_err(), "axis {axis}");
        }
    }

    #[test]
    fn helpers_preserve_matching_acl_and_submit_error_fields() {
        let matched = matching_acl_result(&acl("orders", "User:alice", AclOperation::Read));
        let expected_matched = DeleteAclsMatchingAcl {
            error_code: codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "orders".into(),
            pattern_type: PATTERN_TYPE_LITERAL,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(matched == expected_matched);

        let mut prefixed_acl = acl("orders-", "User:bob", AclOperation::Write);
        prefixed_acl.pattern_type = PatternType::Prefixed;
        let matched = matching_acl_result(&prefixed_acl);
        assert!(matched.pattern_type == PATTERN_TYPE_PREFIXED);
        assert!(matched.operation == OPERATION_WRITE);

        let mut results = vec![
            filter_result(codes::NONE, None, vec![matched.clone()]),
            filter_result(
                codes::INVALID_REQUEST,
                Some("malformed filter axis".into()),
                Vec::new(),
            ),
        ];
        apply_submit_error(&mut results, "not controller");

        let expected_results = vec![
            DeleteAclsFilterResult {
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: not controller".into()),
                matching_acls: vec![matched],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            DeleteAclsFilterResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("malformed filter axis".into()),
                matching_acls: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(results == expected_results);
    }

    #[test]
    fn deleted_acl_resources_include_only_successful_matches() {
        let ok = filter_result(
            codes::NONE,
            None,
            vec![matching_acl_result(&acl(
                "orders",
                "User:alice",
                AclOperation::Read,
            ))],
        );
        let failed = filter_result(
            codes::COORDINATOR_NOT_AVAILABLE,
            Some("submit failed".into()),
            vec![matching_acl_result(&acl(
                "payments",
                "User:bob",
                AclOperation::Write,
            ))],
        );

        let resources = deleted_acl_resources(&[ok, failed]);

        let expected = vec![crabka_audit::AuditResource {
            resource_type: "Acl".into(),
            name: "orders".into(),
        }];
        assert!(resources == expected);
    }

    #[test]
    fn audit_deleted_acls_skips_empty_and_emits_non_empty_admin_event() {
        let (log, mut rx) = crabka_audit::AuditLog::new(8);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        audit_deleted_acls(log.as_ref(), &ctx, Vec::new());
        assert!(
            rx.try_recv().is_err(),
            "empty audit resource list is a no-op"
        );

        audit_deleted_acls(
            log.as_ref(),
            &ctx,
            vec![crabka_audit::AuditResource {
                resource_type: "Acl".into(),
                name: "orders".into(),
            }],
        );

        let event = rx.try_recv().expect("admin audit event");
        let crabka_audit::AuditEvent::AdminOperation {
            outcome,
            principal,
            operation,
            resources,
            ..
        } = event
        else {
            panic!("expected AdminOperation");
        };
        assert!(
            (
                outcome,
                principal.name.as_str(),
                operation.as_str(),
                resources
            ) == (
                crabka_audit::AuditOutcome::Success,
                "admin",
                "DeleteAcls",
                vec![crabka_audit::AuditResource {
                    resource_type: "Acl".into(),
                    name: "orders".into(),
                }],
            )
        );
    }

    #[test]
    fn encode_response_writes_decodable_filter_results() {
        let bytes = encode_response(
            &delete_acls_response(vec![filter_result(
                codes::INVALID_REQUEST,
                Some("malformed filter axis".into()),
                Vec::new(),
            )]),
            VERSION,
        )
        .expect("encode");
        let resp = decode_response(&bytes);

        let expected = DeleteAclsResponse {
            throttle_time_ms: 0,
            filter_results: vec![DeleteAclsFilterResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("malformed filter axis".into()),
                matching_acls: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_for_each_filter() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        seed_acls(
            &broker_handle,
            vec![acl("orders", "User:alice", AclOperation::Read)],
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = request(vec![
            filter(Some("orders"), Some("User:alice")),
            filter(Some("payments"), Some("User:bob")),
        ]);

        let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
        let resp = decode_response(&resp);

        let denied = DeleteAclsFilterResult {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("delete-acls denied".into()),
            matching_acls: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let expected = DeleteAclsResponse {
            throttle_time_ms: 0,
            filter_results: vec![denied.clone(), denied],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(all_acls(&broker_handle).len() == 1);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_returns_matching_acl_fields_and_deletes_only_matches() {
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
        let req = request(vec![filter(Some("orders"), Some("User:alice"))]);

        let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
        let resp = decode_response(&resp);

        let expected = DeleteAclsResponse {
            throttle_time_ms: 0,
            filter_results: vec![DeleteAclsFilterResult {
                error_code: codes::NONE,
                error_message: None,
                matching_acls: vec![DeleteAclsMatchingAcl {
                    error_code: codes::NONE,
                    error_message: None,
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: "orders".into(),
                    pattern_type: PATTERN_TYPE_LITERAL,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: OPERATION_READ,
                    permission_type: PERMISSION_ALLOW,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);

        let remaining = all_acls(&broker_handle);
        assert!(remaining == vec![acl("payments", "User:bob", AclOperation::Write)]);
        broker_handle.shutdown().await;
    }
}
