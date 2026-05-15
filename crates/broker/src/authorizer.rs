//! Slice 13. Pure-logic ACL authorization decision algorithm.
//!
//! Mirrors Kafka's `StandardAuthorizer`:
//! - super-user bypass
//! - compatibility shim: when zero ACLs AND no super-user, ALLOW
//!   (preserves slice 11/12 pre-ACL behavior)
//! - DENY-wins over ALLOW
//! - LITERAL exact + PREFIXED prefix matching on `resource_name`
//! - principal wildcard (`User:*`), host wildcard (`*`),
//!   operation wildcard (`AclOperation::All`)
//! - default-deny when ACLs exist but none match this request

use std::net::SocketAddr;

use crabka_metadata::{AclEntry, AclOperation, MetadataImage, PermissionType, ResourceType};
use crabka_security::Principal;

#[derive(Debug, Clone)]
pub struct AuthorizationRequest<'a> {
    pub principal: &'a Principal,
    pub host: &'a SocketAddr,
    pub resource_type: ResourceType,
    pub resource_name: &'a str,
    pub operation: AclOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationResult {
    Allow,
    Deny,
}

/// Decide whether `req.principal` may perform `req.operation` on
/// `(req.resource_type, req.resource_name)` from `req.host`, given the
/// current metadata image and optional super-user.
///
/// See module docs for the algorithm.
#[must_use]
pub fn authorize(
    image: &MetadataImage,
    super_user_name: Option<&str>,
    req: &AuthorizationRequest,
) -> AuthorizationResult {
    // Compatibility shim: pre-ACL deployments (no super-user, no ACLs)
    // get the old "allow everything authenticated" behavior. Keeps
    // slice 11/12 tests green.
    if super_user_name.is_none() && image.all_acls().next().is_none() {
        return AuthorizationResult::Allow;
    }

    // Super-user bypass. Principal name format is `User:<name>` so we
    // compare against `User:<super>`.
    if let Some(name) = super_user_name
        && req.principal.name == name
    {
        return AuthorizationResult::Allow;
    }

    let user_pattern = format!("User:{}", req.principal.name);
    let host_str = req.host.ip().to_string();
    let mut saw_allow = false;
    for entry in image.matching_acls(req.resource_type, req.resource_name) {
        if !matches_principal(entry, &user_pattern)
            || !matches_host(entry, &host_str)
            || !matches_operation(entry, req.operation)
        {
            continue;
        }
        match entry.permission_type {
            PermissionType::Deny => return AuthorizationResult::Deny,
            PermissionType::Allow => saw_allow = true,
        }
    }
    if saw_allow {
        AuthorizationResult::Allow
    } else {
        AuthorizationResult::Deny
    }
}

fn matches_principal(entry: &AclEntry, user_pattern: &str) -> bool {
    entry.principal == "User:*" || entry.principal == user_pattern
}

fn matches_host(entry: &AclEntry, host: &str) -> bool {
    entry.host == "*" || entry.host == host
}

fn matches_operation(entry: &AclEntry, op: AclOperation) -> bool {
    matches!(entry.operation, AclOperation::All) || entry.operation == op
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataRecord, PatternType};
    use crabka_security::SaslMechanism;
    use uuid::Uuid;

    fn alice() -> Principal {
        Principal {
            name: "alice".into(),
            mechanism: SaslMechanism::Plain,
        }
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    fn img() -> MetadataImage {
        MetadataImage::new(Uuid::nil())
    }

    fn topic_acl(
        permission: PermissionType,
        op: AclOperation,
        principal: &str,
        host: &str,
        pattern: PatternType,
        name: &str,
    ) -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: name.into(),
            pattern_type: pattern,
            principal: principal.into(),
            host: host.into(),
            operation: op,
            permission_type: permission,
        }
    }

    fn req<'a>(
        p: &'a Principal,
        host: &'a SocketAddr,
        name: &'a str,
        op: AclOperation,
    ) -> AuthorizationRequest<'a> {
        AuthorizationRequest {
            principal: p,
            host,
            resource_type: ResourceType::Topic,
            resource_name: name,
            operation: op,
        }
    }

    #[test]
    fn compatibility_shim_allows_when_no_acls_and_no_super_user() {
        let img = img();
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn super_user_bypass_grants_everything_even_with_acls() {
        let mut img = img();
        // A DENY ACL that would otherwise reject.
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Deny,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, Some("alice"), &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn deny_by_default_when_super_user_set_but_principal_mismatches() {
        let mut img = img();
        // Need at least one ACL to disable the compatibility shim.
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:bob",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, Some("admin"), &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Deny,
        );
    }

    #[test]
    fn literal_allow_matches_exact_name() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "foobar", AclOperation::Read)),
            AuthorizationResult::Deny,
        );
    }

    #[test]
    fn prefixed_allow_matches_prefix() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Prefixed,
            "team-",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "team-foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "other", AclOperation::Read)),
            AuthorizationResult::Deny,
        );
    }

    #[test]
    fn deny_wins_over_allow() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Literal,
            "foo",
        )));
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Deny,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Deny,
        );
    }

    #[test]
    fn principal_wildcard_matches_any_user() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:*",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn host_filter_matches_specific_ip() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:alice",
            "127.0.0.1",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h_match: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let h_nomatch: SocketAddr = "127.0.0.2:5000".parse().unwrap();
        assert_eq!(
            authorize(&img, None, &req(&a, &h_match, "foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(&img, None, &req(&a, &h_nomatch, "foo", AclOperation::Read)),
            AuthorizationResult::Deny,
        );
    }

    #[test]
    fn operation_all_matches_any_op() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::All,
            "User:alice",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        for op in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Describe,
            AclOperation::Delete,
        ] {
            assert_eq!(
                authorize(&img, None, &req(&a, &h, "foo", op)),
                AuthorizationResult::Allow,
                "{op:?} should be allowed under operation::All",
            );
        }
    }

    #[test]
    fn operation_specific_does_not_match_others() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(&img, None, &req(&a, &h, "foo", AclOperation::Write)),
            AuthorizationResult::Deny,
        );
    }
}
