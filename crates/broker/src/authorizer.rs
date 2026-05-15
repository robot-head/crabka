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

/// Batch-authorize a set of topic names against the same principal /
/// host / operation. Used by `Produce`, `Fetch`, and `Metadata`
/// per-topic enforcement. The returned map's keys are borrowed from
/// the input iterator so callers can avoid copying topic strings.
#[must_use]
pub fn authorize_topics<'a, S: std::hash::BuildHasher>(
    image: &MetadataImage,
    super_users: &std::collections::HashSet<String, S>,
    principal: &Principal,
    host: &SocketAddr,
    operation: AclOperation,
    topic_names: impl IntoIterator<Item = &'a str>,
) -> std::collections::HashMap<&'a str, AuthorizationResult> {
    topic_names
        .into_iter()
        .map(|name| {
            let req = AuthorizationRequest {
                principal,
                host,
                resource_type: ResourceType::Topic,
                resource_name: name,
                operation,
            };
            (name, authorize(image, super_users, &req))
        })
        .collect()
}

/// Decide whether `req.principal` may perform `req.operation` on
/// `(req.resource_type, req.resource_name)` from `req.host`, given the
/// current metadata image and optional super-user.
///
/// See module docs for the algorithm.
#[must_use]
pub fn authorize<S: std::hash::BuildHasher>(
    image: &MetadataImage,
    super_users: &std::collections::HashSet<String, S>,
    req: &AuthorizationRequest,
) -> AuthorizationResult {
    // Compatibility shim: pre-ACL deployments (no super-user, no ACLs)
    // get the old "allow everything authenticated" behavior. Keeps
    // slice 11/12 tests green.
    if super_users.is_empty() && image.all_acls().next().is_none() {
        return AuthorizationResult::Allow;
    }

    // Super-user bypass.
    if super_users.contains(&req.principal.name) {
        return AuthorizationResult::Allow;
    }

    let user_pattern = format!("User:{}", req.principal.name);
    let host_str = req.host.ip().to_string();
    let mut saw_allow = false;
    for entry in image.matching_acls(req.resource_type, req.resource_name) {
        if !matches_principal(entry, &user_pattern)
            || !matches_host(entry, &host_str)
            || !matches_operation(entry.operation, req.operation)
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

/// Returns true when an ACL with `stored` operation grants access for
/// an authorization request with `requested` operation. Beyond exact
/// match and the `All` wildcard, applies Kafka's operation-implication
/// table:
///
/// | stored          | implies                |
/// |-----------------|------------------------|
/// | Read            | Describe               |
/// | Write           | Describe               |
/// | Delete          | Describe               |
/// | Alter           | Describe               |
/// | `AlterConfigs`  | `DescribeConfigs`      |
/// | All             | Everything             |
///
/// The table is one-way: Describe does NOT imply Read, etc.
fn matches_operation(stored: AclOperation, requested: AclOperation) -> bool {
    if stored == requested {
        return true;
    }
    if matches!(stored, AclOperation::All) {
        return true;
    }
    implies(stored, requested)
}

fn implies(stored: AclOperation, requested: AclOperation) -> bool {
    matches!(
        (stored, requested),
        (
            AclOperation::Read | AclOperation::Write | AclOperation::Delete | AclOperation::Alter,
            AclOperation::Describe,
        ) | (AclOperation::AlterConfigs, AclOperation::DescribeConfigs)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataRecord, PatternType};
    use crabka_security::SaslMechanism;
    use uuid::Uuid;

    fn no_super() -> std::collections::HashSet<String> {
        std::collections::HashSet::new()
    }
    fn one_super(name: &str) -> std::collections::HashSet<String> {
        let mut s = std::collections::HashSet::new();
        s.insert(name.to_string());
        s
    }

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
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Read)),
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
            authorize(
                &img,
                &one_super("alice"),
                &req(&a, &h, "foo", AclOperation::Read)
            ),
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
            authorize(
                &img,
                &one_super("admin"),
                &req(&a, &h, "foo", AclOperation::Read)
            ),
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
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(
                &img,
                &no_super(),
                &req(&a, &h, "foobar", AclOperation::Read)
            ),
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
            authorize(
                &img,
                &no_super(),
                &req(&a, &h, "team-foo", AclOperation::Read)
            ),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "other", AclOperation::Read)),
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
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Read)),
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
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Read)),
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
            authorize(
                &img,
                &no_super(),
                &req(&a, &h_match, "foo", AclOperation::Read)
            ),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(
                &img,
                &no_super(),
                &req(&a, &h_nomatch, "foo", AclOperation::Read)
            ),
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
                authorize(&img, &no_super(), &req(&a, &h, "foo", op)),
                AuthorizationResult::Allow,
                "{op:?} should be allowed under operation::All",
            );
        }
    }

    #[test]
    fn authorize_topics_batch_returns_per_topic_decisions() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Literal,
            "t1",
        )));
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Deny,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Literal,
            "t2",
        )));
        let a = alice();
        let h = addr();
        let map = authorize_topics(
            &img,
            &no_super(),
            &a,
            &h,
            AclOperation::Read,
            ["t1", "t2", "t3"],
        );
        assert_eq!(map.get("t1").copied(), Some(AuthorizationResult::Allow));
        assert_eq!(map.get("t2").copied(), Some(AuthorizationResult::Deny));
        // t3: no matching ACL + non-empty image → Deny by default.
        assert_eq!(map.get("t3").copied(), Some(AuthorizationResult::Deny));
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
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Write)),
            AuthorizationResult::Deny,
        );
    }

    fn topic_acl_op(permission: PermissionType, op: AclOperation, name: &str) -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: name.into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: op,
            permission_type: permission,
        }
    }

    #[test]
    fn read_implies_describe_on_topic() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Read,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(
                &img,
                &no_super(),
                &req(&a, &h, "foo", AclOperation::Describe)
            ),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn write_implies_describe_on_topic() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Write,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(
                &img,
                &no_super(),
                &req(&a, &h, "foo", AclOperation::Describe)
            ),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn delete_implies_describe() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Delete,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(
                &img,
                &no_super(),
                &req(&a, &h, "foo", AclOperation::Describe)
            ),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn alter_implies_describe() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Alter,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(
                &img,
                &no_super(),
                &req(&a, &h, "foo", AclOperation::Describe)
            ),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn alter_configs_implies_describe_configs() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::AlterConfigs,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(
                &img,
                &no_super(),
                &req(&a, &h, "foo", AclOperation::DescribeConfigs)
            ),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn describe_does_not_imply_read() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Describe,
            "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Deny,
        );
    }
}
