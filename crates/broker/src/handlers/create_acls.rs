//! `CreateAcls` handler (`api_key` 30).
//!
//! Authorizes `Alter` on `Cluster`. For each binding, validates
//! the resource shape and submits a `V1AccessControlEntry` to the
//! controller. Returns per-binding results.

use bytes::Bytes;
use crabka_metadata::{AclEntry, MetadataRecord};
use crabka_protocol::Encode;
use crabka_protocol::owned::create_acls_request::CreateAclsRequest;
use crabka_protocol::owned::create_acls_response::{AclCreationResult, CreateAclsResponse};

use super::acl_wire::{
    operation_concrete, pattern_type_concrete, permission_concrete, resource_type_concrete,
};
use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;

const MAX_PRINCIPAL_LEN: usize = 256;
const MAX_RESOURCE_NAME_LEN: usize = 256;

#[tracing::instrument(
    name = "handle_create_acls",
    level = "info",
    skip_all,
    fields(api = "CreateAcls"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: CreateAclsRequest,
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
        let results = req
            .creations
            .iter()
            .map(|_| AclCreationResult {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("create-acls denied".into()),
                ..Default::default()
            })
            .collect();
        return encode_response(
            &CreateAclsResponse {
                throttle_time_ms: 0,
                results,
                ..Default::default()
            },
            api_version,
        );
    }

    let mut results: Vec<AclCreationResult> = Vec::with_capacity(req.creations.len());
    let mut to_submit: Vec<(usize, MetadataRecord)> = Vec::with_capacity(req.creations.len());

    for c in &req.creations {
        match validate(c) {
            Ok(entry) => {
                let idx = results.len();
                results.push(AclCreationResult::default());
                to_submit.push((idx, MetadataRecord::V1AccessControlEntry(entry)));
            }
            Err((code, msg)) => {
                results.push(AclCreationResult {
                    error_code: code,
                    error_message: Some(msg.into()),
                    ..Default::default()
                });
            }
        }
    }

    if !to_submit.is_empty() {
        let records: Vec<MetadataRecord> = to_submit.iter().map(|(_, r)| r.clone()).collect();
        if let Err(e) = broker.controller.submit_change(records).await {
            tracing::warn!(error = %e, "create-acls submit failed");
            for (idx, _) in &to_submit {
                results[*idx] = AclCreationResult {
                    error_code: codes::COORDINATOR_NOT_AVAILABLE,
                    error_message: Some(format!("submit failed: {e}")),
                    ..Default::default()
                };
            }
        }
    }

    // Audit: emit one AdminOperation record for successfully-created ACLs.
    // `to_submit` carries (result_idx, record) for every creation that passed
    // validation; entries whose result slot still has error_code == 0 were committed.
    let created_acls: Vec<crabka_audit::AuditResource> = to_submit
        .iter()
        .filter(|(idx, _)| results[*idx].error_code == 0)
        .map(|(idx, _)| crabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: req.creations[*idx].resource_name.clone(),
        })
        .collect();
    if !created_acls.is_empty() {
        crate::handlers::audit_admin(
            broker.audit_log.as_ref(),
            ctx,
            "CreateAcls",
            crabka_audit::AuditOutcome::Success,
            created_acls,
        );
    }

    encode_response(
        &CreateAclsResponse {
            throttle_time_ms: 0,
            results,
            ..Default::default()
        },
        api_version,
    )
}

fn validate(
    c: &crabka_protocol::owned::create_acls_request::AclCreation,
) -> Result<AclEntry, (i16, &'static str)> {
    let resource_type = resource_type_concrete(c.resource_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad resource_type"))?;
    let pattern_type = pattern_type_concrete(c.resource_pattern_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad pattern_type"))?;
    let operation =
        operation_concrete(c.operation).map_err(|_| (codes::INVALID_REQUEST, "bad operation"))?;
    let permission_type = permission_concrete(c.permission_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad permission_type"))?;

    if c.resource_name.is_empty() {
        return Err((codes::INVALID_REQUEST, "empty resource_name"));
    }
    if c.resource_name.len() > MAX_RESOURCE_NAME_LEN {
        return Err((codes::INVALID_REQUEST, "resource_name too long"));
    }
    if c.resource_name.contains('\0') {
        return Err((codes::INVALID_REQUEST, "resource_name contains NUL"));
    }
    if !c.principal.starts_with("User:") {
        return Err((codes::INVALID_REQUEST, "principal must start with User:"));
    }
    if c.principal.len() > MAX_PRINCIPAL_LEN {
        return Err((codes::INVALID_REQUEST, "principal too long"));
    }
    if c.host.is_empty() {
        return Err((codes::INVALID_REQUEST, "empty host"));
    }
    Ok(AclEntry {
        resource_type,
        resource_name: c.resource_name.clone(),
        pattern_type,
        principal: c.principal.clone(),
        host: c.host.clone(),
        operation,
        permission_type,
    })
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode CreateAcls: {e}")))?;
    Ok(Bytes::from(body))
}
