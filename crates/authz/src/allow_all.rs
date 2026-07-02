//! Default authorizer when authorization is unset. Returns `Allow` for
//! any request. Provides an explicit type so the "allow everything"
//! behavior is spelled out at config time rather than emerging from the
//! ACL impl's empty-input path.

use crate::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer};

/// Authorizer that always returns [`AuthorizationResult::Allow`].
/// Default authorizer value; chosen by `type = "allow_all"` (or omitted
/// entirely) in the broker / gateway config.
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

    use assert2::assert;
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
        assert!(AllowAllAuthorizer.authorize(&img, &req) == AuthorizationResult::Allow);
    }
}
