//! Authorizer decorator that records every Deny decision as an audit event.

use std::sync::Arc;

use crabka_audit::{AuditEndpoint, AuditEvent, AuditLog, AuditPrincipal};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer};

/// Wraps an [`Authorizer`]. It forwards decisions and emits an audit record on
/// every Deny. The handlers audit Allow decisions for admin operations
/// separately (Task 8), so this decorator does not duplicate them.
#[derive(Debug)]
pub struct AuditingAuthorizer {
    inner: Arc<dyn Authorizer>,
    audit: Arc<AuditLog>,
}

impl AuditingAuthorizer {
    #[must_use]
    pub fn new(inner: Arc<dyn Authorizer>, audit: Arc<AuditLog>) -> Self {
        Self { inner, audit }
    }
}

impl Authorizer for AuditingAuthorizer {
    fn authorize(
        &self,
        source: &dyn crabka_authz::AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let result = self.inner.authorize(source, req);
        if result == AuthorizationResult::Deny {
            self.audit.emit(AuditEvent::AuthorizationDenied {
                principal: AuditPrincipal {
                    name: req.principal.name.clone(),
                    auth_method: format!("{:?}", req.principal.auth_method),
                },
                source: AuditEndpoint {
                    ip: req.host.ip().to_string(),
                    port: req.host.port(),
                },
                resource_type: format!("{:?}", req.resource_type),
                resource_name: req.resource_name.to_string(),
                operation: format!("{:?}", req.operation),
                time_ms: crate::time_util::now_ms(),
            });
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::check;
    use crabka_metadata::{AclOperation, ResourceType};
    use crabka_security::{AuthMethod, Principal};

    use super::*;
    use crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        test_support::DenyAll,
    };

    #[tokio::test]
    async fn deny_decision_emits_audit_record() {
        let (log, mut rx) = crabka_audit::AuditLog::new(8);
        let authz = AuditingAuthorizer::new(Arc::new(DenyAll), log);

        let principal = Principal {
            name: "anonymous".into(),
            auth_method: AuthMethod::Anonymous,
            groups: vec![],
        };
        let host: SocketAddr = "10.0.0.9:5555".parse().unwrap();
        let image = crabka_metadata::MetadataImage::default();
        let result = authz.authorize(
            &image,
            &AuthorizationRequest {
                principal: &principal,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "secrets",
                operation: AclOperation::Write,
            },
        );
        check!(result == AuthorizationResult::Deny);

        let ev = rx.try_recv().expect("an audit event was emitted");
        match ev {
            crabka_audit::AuditEvent::AuthorizationDenied {
                resource_name,
                operation,
                ..
            } => {
                check!(resource_name == "secrets");
                check!(operation == "Write");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
