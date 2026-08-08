//! Default authorizer when authorization is unset.
//!
//! This authorizer returns `Allow` for any request. It is an explicit type, so
//! the "allow everything" behavior is clear at config time. The behavior does
//! not come from the empty-input path of the ACL implementation.

use crate::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer};

/// Authorizer that always returns [`AuthorizationResult::Allow`].
///
/// This is the default authorizer value. An operator selects it with
/// `type = "allow_all"` in the broker or gateway config, or by omission of the
/// field.
#[derive(Debug, Default)]
pub struct AllowAllAuthorizer;

impl Authorizer for AllowAllAuthorizer {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        _req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        AuthorizationResult::Allow
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
    use crabka_security::{AuthMethod, Principal};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn allow_all_returns_allow_for_any_request() {
        let img = MetadataImage::new(Uuid::nil());
        let p = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec![],
        };
        let host: SocketAddr = "1.2.3.4:9092".parse().unwrap();
        let req = AuthorizationRequest {
            principal: &p,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "anything",
            operation: AclOperation::Write,
        };
        assert2::assert!(AllowAllAuthorizer.authorize(&img, &req) == AuthorizationResult::Allow);
    }
}
