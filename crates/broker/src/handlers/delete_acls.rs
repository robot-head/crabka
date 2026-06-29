//! `DeleteAcls` handler (`api_key` 31).

use bytes::Bytes;
use crabka_metadata::{AclEntry, AclEntryFilter, MetadataRecord};
use crabka_protocol::Encode;
use crabka_protocol::owned::delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest};
use crabka_protocol::owned::delete_acls_response::{
    DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse,
};

use super::acl_wire::{
    operation_filter, operation_to_wire, pattern_type_filter, pattern_type_to_wire,
    permission_filter, permission_to_wire, resource_type_filter, resource_type_to_wire,
};
use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;

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
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if allow == AuthorizationResult::Deny {
        let filter_results = req
            .filters
            .iter()
            .map(|_| DeleteAclsFilterResult {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("delete-acls denied".into()),
                matching_acls: vec![],
                ..Default::default()
            })
            .collect();
        return encode_response(
            &DeleteAclsResponse {
                throttle_time_ms: 0,
                filter_results,
                ..Default::default()
            },
            api_version,
        );
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
                    .map(|e| DeleteAclsMatchingAcl {
                        error_code: 0,
                        error_message: None,
                        resource_type: resource_type_to_wire(e.resource_type),
                        resource_name: e.resource_name.clone(),
                        pattern_type: pattern_type_to_wire(e.pattern_type),
                        principal: e.principal.clone(),
                        host: e.host.clone(),
                        operation: operation_to_wire(e.operation),
                        permission_type: permission_to_wire(e.permission_type),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>();
                filter_results.push(DeleteAclsFilterResult {
                    error_code: 0,
                    error_message: None,
                    matching_acls,
                    ..Default::default()
                });
                to_submit.push(MetadataRecord::V1DeleteAccessControlEntry(filter));
            }
            Err(_) => {
                filter_results.push(DeleteAclsFilterResult {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some("malformed filter axis".into()),
                    matching_acls: vec![],
                    ..Default::default()
                });
            }
        }
    }

    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "delete-acls submit failed");
        for r in &mut filter_results {
            if r.error_code == 0 {
                r.error_code = codes::COORDINATOR_NOT_AVAILABLE;
                r.error_message = Some(format!("submit failed: {e}"));
            }
        }
    }

    // Audit: emit one AdminOperation record for successfully-deleted ACLs.
    // Collect resource_name from each matching ACL in every filter result that
    // committed without error (error_code == 0).
    let deleted_acls: Vec<crabka_audit::AuditResource> = filter_results
        .iter()
        .filter(|r| r.error_code == 0)
        .flat_map(|r| r.matching_acls.iter())
        .map(|m| crabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: m.resource_name.clone(),
        })
        .collect();
    if !deleted_acls.is_empty() {
        crate::handlers::audit_admin(
            broker.audit_log.as_ref(),
            ctx,
            "DeleteAcls",
            crabka_audit::AuditOutcome::Success,
            deleted_acls,
        );
    }

    encode_response(
        &DeleteAclsResponse {
            throttle_time_ms: 0,
            filter_results,
            ..Default::default()
        },
        api_version,
    )
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
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode DeleteAcls: {e}")))?;
    Ok(Bytes::from(body))
}
