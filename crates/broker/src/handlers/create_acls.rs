//! `CreateAcls` handler (`api_key` 30).
//!
//! Authorizes `Alter` on `Cluster`. For each binding, validates
//! the resource shape and submits a `V1AccessControlEntry` to the
//! controller. Returns per-binding results.

use bytes::Bytes;
use crabka_metadata::{AclEntry, MetadataRecord};
use crabka_protocol::{
    Encode,
    owned::{
        create_acls_request::CreateAclsRequest,
        create_acls_response::{AclCreationResult, CreateAclsResponse},
    },
};

use super::acl_wire::{
    CLUSTER_RESOURCE_NAME, operation_concrete, pattern_type_concrete, permission_concrete,
    resource_type_concrete,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// Maximum accepted length (bytes) of an ACL principal string.
const MAX_PRINCIPAL_LEN: usize = 256;
/// Maximum accepted length (bytes) of an ACL resource name.
const MAX_RESOURCE_NAME_LEN: usize = 256;
/// Kafka principal-type prefix; the only principal type Crabka accepts.
const USER_PRINCIPAL_PREFIX: &str = "User:";

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
            resource_name: CLUSTER_RESOURCE_NAME,
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if allow == AuthorizationResult::Deny {
        let results = req
            .creations
            .iter()
            .map(|_| acl_error_result(codes::CLUSTER_AUTHORIZATION_FAILED, "create-acls denied"))
            .collect();
        return encode_response(&create_acls_response(results), api_version);
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
                results.push(acl_error_result(code, msg));
            }
        }
    }

    if !to_submit.is_empty() {
        let records: Vec<MetadataRecord> = to_submit.iter().map(|(_, r)| r.clone()).collect();
        if let Err(e) = broker.controller.submit_change(records).await {
            tracing::warn!(error = %e, "create-acls submit failed");
            apply_submit_error(&mut results, &to_submit, &e);
        }
    }

    // Audit: emit one AdminOperation record for successfully-created ACLs.
    // `to_submit` carries (result_idx, record) for every creation that passed
    // validation; entries whose result slot still has error_code == 0 were committed.
    audit_created_acls(
        broker.audit_log.as_ref(),
        ctx,
        created_acl_resources(&req, &results, &to_submit),
    );

    encode_response(&create_acls_response(results), api_version)
}

fn acl_error_result(code: i16, msg: impl Into<String>) -> AclCreationResult {
    AclCreationResult {
        error_code: code,
        error_message: Some(msg.into()),
        ..Default::default()
    }
}

fn create_acls_response(results: Vec<AclCreationResult>) -> CreateAclsResponse {
    CreateAclsResponse {
        results,
        ..Default::default()
    }
}

fn apply_submit_error<E: std::fmt::Display>(
    results: &mut [AclCreationResult],
    to_submit: &[(usize, MetadataRecord)],
    err: E,
) {
    let msg = format!("submit failed: {err}");
    for (idx, _) in to_submit {
        results[*idx] = acl_error_result(codes::COORDINATOR_NOT_AVAILABLE, msg.clone());
    }
}

fn created_acl_resources(
    req: &CreateAclsRequest,
    results: &[AclCreationResult],
    to_submit: &[(usize, MetadataRecord)],
) -> Vec<crabka_audit::AuditResource> {
    to_submit
        .iter()
        .filter(|(idx, _)| results[*idx].error_code == codes::NONE)
        .map(|(idx, _)| crabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: req.creations[*idx].resource_name.clone(),
        })
        .collect()
}

fn audit_created_acls(
    audit_log: &crabka_audit::AuditLog,
    ctx: &crate::handlers::RequestContext<'_>,
    created_acls: Vec<crabka_audit::AuditResource>,
) {
    if !created_acls.is_empty() {
        crate::handlers::audit_admin(
            audit_log,
            ctx,
            "CreateAcls",
            crabka_audit::AuditOutcome::Success,
            created_acls,
        );
    }
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
    if !c.principal.starts_with(USER_PRINCIPAL_PREFIX) {
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
    crate::handlers::encode_response_with_context(resp, api_version, "encode CreateAcls")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};
    use crabka_protocol::{UnknownTaggedFields, owned::create_acls_request::AclCreation};

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 3;
    const RESOURCE_TYPE_TOPIC: i8 = 2;
    const PATTERN_TYPE_LITERAL: i8 = 3;
    const OPERATION_READ: i8 = 3;
    const OPERATION_WRITE: i8 = 4;
    const PERMISSION_ALLOW: i8 = 3;

    fn creation(resource_name: &str, principal: &str, operation: i8) -> AclCreation {
        AclCreation {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: resource_name.into(),
            resource_pattern_type: PATTERN_TYPE_LITERAL,
            principal: principal.into(),
            host: "*".into(),
            operation,
            permission_type: PERMISSION_ALLOW,
            ..Default::default()
        }
    }

    fn request(creations: Vec<AclCreation>) -> CreateAclsRequest {
        CreateAclsRequest {
            creations,
            ..Default::default()
        }
    }

    crate::test_support::response_helpers!(
        CreateAclsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    fn all_acls(handle: &BrokerHandle) -> Vec<crabka_metadata::AclEntry> {
        handle
            .controller_image_for_test()
            .all_acls()
            .cloned()
            .collect()
    }

    #[test]
    fn validate_accepts_exact_length_boundaries_and_rejects_above_them() {
        let resource_name = "r".repeat(MAX_RESOURCE_NAME_LEN);
        let principal_name = format!("User:{}", "a".repeat(MAX_PRINCIPAL_LEN - "User:".len()));
        let c = creation(&resource_name, &principal_name, OPERATION_READ);

        let entry = validate(&c).expect("exact boundary lengths are valid");
        let expected = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: resource_name.clone(),
            pattern_type: PatternType::Literal,
            principal: principal_name.clone(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        assert2::assert!(entry == expected);

        let mut too_long_resource = c.clone();
        too_long_resource.resource_name = "r".repeat(MAX_RESOURCE_NAME_LEN + 1);
        let err = validate(&too_long_resource).unwrap_err();
        assert2::assert!(err == (codes::INVALID_REQUEST, "resource_name too long"));

        let mut too_long_principal = c;
        too_long_principal.principal =
            format!("User:{}", "a".repeat(MAX_PRINCIPAL_LEN + 1 - "User:".len()));
        let err = validate(&too_long_principal).unwrap_err();
        assert2::assert!(err == (codes::INVALID_REQUEST, "principal too long"));
    }

    #[test]
    fn validate_rejects_malformed_resource_principal_and_host() {
        type TestCase1 = (fn(&mut AclCreation), &'static str);
        let valid = creation("topic-a", "User:alice", OPERATION_READ);
        let entry = validate(&valid).expect("valid ACL creation");
        let expected = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "topic-a".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        assert2::assert!(entry == expected);

        let cases: [TestCase1; 4] = [
            (|c| c.resource_name.clear(), "empty resource_name"),
            (
                |c| c.resource_name = "bad\0name".into(),
                "resource_name contains NUL",
            ),
            (
                |c| c.principal = "alice".into(),
                "principal must start with User:",
            ),
            (|c| c.host.clear(), "empty host"),
        ];
        for (corrupt, want) in cases {
            let mut c = valid.clone();
            corrupt(&mut c);
            assert2::assert!(validate(&c).unwrap_err().1 == want);
        }
    }

    #[test]
    fn error_and_submit_helpers_preserve_non_default_result_fields() {
        let err = acl_error_result(codes::INVALID_REQUEST, "bad acl");
        let expected_err = AclCreationResult {
            error_code: codes::INVALID_REQUEST,
            error_message: Some("bad acl".into()),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert2::assert!(err == expected_err);

        let mut results = vec![
            AclCreationResult::default(),
            acl_error_result(codes::INVALID_REQUEST, "already invalid"),
        ];
        let submitted = vec![(
            0usize,
            MetadataRecord::V1AccessControlEntry(
                validate(&creation("topic-a", "User:alice", OPERATION_READ))
                    .expect("valid creation"),
            ),
        )];

        apply_submit_error(&mut results, &submitted, "not controller");

        let expected = vec![
            AclCreationResult {
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: not controller".into()),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
            AclCreationResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("already invalid".into()),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
        ];
        assert2::assert!(results == expected);
    }

    #[test]
    fn created_acl_resources_include_only_successful_submitted_creations() {
        let req = request(vec![
            creation("topic-ok", "User:alice", OPERATION_READ),
            creation("topic-bad", "User:bob", OPERATION_WRITE),
        ]);
        let submitted = vec![
            (
                0usize,
                MetadataRecord::V1AccessControlEntry(validate(&req.creations[0]).unwrap()),
            ),
            (
                1usize,
                MetadataRecord::V1AccessControlEntry(validate(&req.creations[1]).unwrap()),
            ),
        ];
        let results = vec![
            AclCreationResult::default(),
            acl_error_result(codes::COORDINATOR_NOT_AVAILABLE, "submit failed"),
        ];

        let resources = created_acl_resources(&req, &results, &submitted);

        let expected = vec![crabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: "topic-ok".to_string(),
        }];
        assert2::assert!(resources == expected);
    }

    #[test]
    fn audit_created_acls_skips_empty_and_emits_non_empty_admin_event() {
        let (log, mut rx) = crabka_audit::AuditLog::new(8);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        audit_created_acls(log.as_ref(), &ctx, Vec::new());
        assert2::assert!(rx.try_recv().is_err());

        audit_created_acls(
            log.as_ref(),
            &ctx,
            vec![crabka_audit::AuditResource {
                resource_type: "Acl".into(),
                name: "topic-ok".into(),
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
        assert2::assert!(
            (
                outcome,
                principal.name.as_str(),
                operation.as_str(),
                resources,
            ) == (
                crabka_audit::AuditOutcome::Success,
                "admin",
                "CreateAcls",
                vec![crabka_audit::AuditResource {
                    resource_type: "Acl".to_string(),
                    name: "topic-ok".to_string(),
                }],
            )
        );
    }

    #[test]
    fn encode_response_writes_decodable_results() {
        let bytes = encode_response(
            &create_acls_response(vec![acl_error_result(codes::INVALID_REQUEST, "bad acl")]),
            VERSION,
        )
        .expect("encode");
        let decoded = decode_response(&bytes);

        let expected = CreateAclsResponse {
            throttle_time_ms: 0,
            results: vec![AclCreationResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("bad acl".into()),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert2::assert!(decoded == expected);
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_for_each_creation() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = request(vec![
            creation("topic-a", "User:bob", OPERATION_READ),
            creation("topic-b", "User:carol", OPERATION_WRITE),
        ]);

        let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
        let resp = decode_response(&resp);

        let denied = AclCreationResult {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("create-acls denied".into()),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        let expected = CreateAclsResponse {
            throttle_time_ms: 0,
            results: vec![denied.clone(), denied],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert2::assert!(resp == expected);
        assert2::assert!(all_acls(&broker_handle).is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_submits_valid_creations_and_reports_invalid_creations_in_order() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let mut invalid = creation("", "User:bob", OPERATION_WRITE);
        invalid.resource_name.clear();
        let req = request(vec![
            creation("topic-a", "User:alice", OPERATION_READ),
            invalid,
        ]);

        let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
        let resp = decode_response(&resp);

        let expected = CreateAclsResponse {
            throttle_time_ms: 0,
            results: vec![
                AclCreationResult {
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
                AclCreationResult {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some("empty resource_name".into()),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert2::assert!(resp == expected);

        let acls = all_acls(&broker_handle);
        let expected_acls = vec![AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "topic-a".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }];
        assert2::assert!(acls == expected_acls);
        broker_handle.shutdown().await;
    }
}
