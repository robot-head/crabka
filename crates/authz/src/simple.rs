//! ACL-based authorizer behind the [`Authorizer`] trait. Super-user
//! bypass + deny-wins-over-allow + LITERAL/PREFIXED matching +
//! principal/host/operation wildcards.
//!
//! There is no "empty source + no super-users ⇒ Allow" compat shim —
//! that case lives in [`crate::AllowAllAuthorizer`].
//! [`SimpleAclAuthorizer`] with an empty source + empty super-users
//! denies everything (default-deny), which matches Kafka's
//! `StandardAuthorizer` once an authorizer is explicitly configured.

use std::collections::HashSet;

use crabka_metadata::{AclEntry, AclOperation, PermissionType};

use crate::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer};

/// Authorizer that consults the cluster's persisted ACLs (the
/// [`AclSource`] is supplied per call — a `MetadataImage` for the broker,
/// an `AclCache` for the gateway). Holds the configured super-user set;
/// principals in this set bypass ACL evaluation and always get `Allow`.
#[derive(Debug)]
pub struct SimpleAclAuthorizer {
    super_users: HashSet<String>,
}

impl SimpleAclAuthorizer {
    #[must_use]
    pub fn new(super_users: HashSet<String>) -> Self {
        Self { super_users }
    }
}

impl Authorizer for SimpleAclAuthorizer {
    // Per-request ACL decision: skip_all keeps the borrowed principal/host
    // structs (which may carry the raw name) out of span fields; only
    // non-sensitive routing context is recorded. No `err` — this returns a
    // plain Allow/Deny, not a Result. `decision` is filled in before each
    // return path.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            principal = %req.principal.name,
            resource_type = ?req.resource_type,
            resource = %req.resource_name,
            operation = ?req.operation,
            host = %req.host.ip(),
            decision = tracing::field::Empty,
        )
    )]
    fn authorize(
        &self,
        source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let span = tracing::Span::current();
        // Super-user bypass.
        if self.super_users.contains(&req.principal.name) {
            span.record("decision", "allow-superuser");
            return AuthorizationResult::Allow;
        }

        let user_pattern = format!("User:{}", req.principal.name);
        let host_str = req.host.ip().to_string();
        let mut saw_allow = false;
        for entry in source.matching_acls(req.resource_type, req.resource_name) {
            if !matches_principal(entry, &user_pattern)
                || !matches_host(entry, &host_str)
                || !matches_operation(entry.operation, req.operation)
            {
                continue;
            }
            match entry.permission_type {
                PermissionType::Deny => {
                    span.record("decision", "deny-explicit");
                    return AuthorizationResult::Deny;
                }
                PermissionType::Allow => saw_allow = true,
            }
        }
        if saw_allow {
            span.record("decision", "allow-acl");
            AuthorizationResult::Allow
        } else {
            span.record("decision", "deny-default");
            AuthorizationResult::Deny
        }
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
    use crate::authorize_topics;
    use assert2::assert;
    use crabka_metadata::{MetadataImage, MetadataRecord, PatternType, ResourceType};
    use crabka_security::Principal;
    use std::net::SocketAddr;
    use uuid::Uuid;

    fn no_super() -> HashSet<String> {
        HashSet::new()
    }
    fn one_super(name: &str) -> HashSet<String> {
        let mut s = HashSet::new();
        s.insert(name.to_string());
        s
    }

    fn alice() -> Principal {
        Principal {
            name: "alice".into(),
            auth_method: crabka_security::AuthMethod::SaslPlain,
            groups: vec![],
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
    fn empty_image_with_no_super_users_defaults_to_deny() {
        // There is no compat shim that returns Allow in this case —
        // `SimpleAclAuthorizer` is default-deny when nothing matches.
        // Operators who want "allow everything" should configure
        // `AllowAllAuthorizer` explicitly.
        let img = img();
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Deny
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
        let auth = SimpleAclAuthorizer::new(one_super("alice"));
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn deny_by_default_when_super_user_set_but_principal_mismatches() {
        let mut img = img();
        // An ACL exists but doesn't match alice.
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
        let auth = SimpleAclAuthorizer::new(one_super("admin"));
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Deny
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Allow
        );
        assert!(
            auth.authorize(&img, &req(&a, &h, "foobar", AclOperation::Read))
                == AuthorizationResult::Deny
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "team-foo", AclOperation::Read))
                == AuthorizationResult::Allow
        );
        assert!(
            auth.authorize(&img, &req(&a, &h, "other", AclOperation::Read))
                == AuthorizationResult::Deny
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Deny
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Allow
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h_match, "foo", AclOperation::Read))
                == AuthorizationResult::Allow
        );
        assert!(
            auth.authorize(&img, &req(&a, &h_nomatch, "foo", AclOperation::Read))
                == AuthorizationResult::Deny
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
        let auth = SimpleAclAuthorizer::new(no_super());
        for op in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Describe,
            AclOperation::Delete,
        ] {
            assert!(
                auth.authorize(&img, &req(&a, &h, "foo", op)) == AuthorizationResult::Allow,
                "{op:?} should be allowed under operation::All"
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
        let auth = SimpleAclAuthorizer::new(no_super());
        let map = authorize_topics(&auth, &img, &a, &h, AclOperation::Read, ["t1", "t2", "t3"]);
        assert!(map.get("t1").copied() == Some(AuthorizationResult::Allow));
        assert!(map.get("t2").copied() == Some(AuthorizationResult::Deny));
        // t3: no matching ACL → Deny by default (default-deny; there is
        // no shim that allows in this case).
        assert!(map.get("t3").copied() == Some(AuthorizationResult::Deny));
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Allow
        );
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Write))
                == AuthorizationResult::Deny
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Describe))
                == AuthorizationResult::Allow
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Describe))
                == AuthorizationResult::Allow
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Describe))
                == AuthorizationResult::Allow
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Describe))
                == AuthorizationResult::Allow
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::DescribeConfigs))
                == AuthorizationResult::Allow
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
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Deny
        );
    }

    fn acl_op_on(
        rt: ResourceType,
        permission: PermissionType,
        op: AclOperation,
        name: &str,
    ) -> AclEntry {
        AclEntry {
            resource_type: rt,
            resource_name: name.into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: op,
            permission_type: permission,
        }
    }

    fn req_on<'a>(
        p: &'a Principal,
        host: &'a SocketAddr,
        rt: ResourceType,
        name: &'a str,
        op: AclOperation,
    ) -> AuthorizationRequest<'a> {
        AuthorizationRequest {
            principal: p,
            host,
            resource_type: rt,
            resource_name: name,
            operation: op,
        }
    }

    #[test]
    fn implication_works_on_group_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::Group,
            PermissionType::Allow,
            AclOperation::Read,
            "cg-1",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(
                &img,
                &req_on(&a, &h, ResourceType::Group, "cg-1", AclOperation::Describe)
            ) == AuthorizationResult::Allow
        );
    }

    #[test]
    fn implication_works_on_cluster_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::Cluster,
            PermissionType::Allow,
            AclOperation::Alter,
            "kafka-cluster",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(
                &img,
                &req_on(
                    &a,
                    &h,
                    ResourceType::Cluster,
                    "kafka-cluster",
                    AclOperation::Describe
                )
            ) == AuthorizationResult::Allow
        );
    }

    #[test]
    fn implication_works_on_transactional_id_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::TransactionalId,
            PermissionType::Allow,
            AclOperation::Write,
            "tx-1",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert!(
            auth.authorize(
                &img,
                &req_on(
                    &a,
                    &h,
                    ResourceType::TransactionalId,
                    "tx-1",
                    AclOperation::Describe
                )
            ) == AuthorizationResult::Allow
        );
    }

    #[test]
    fn multi_super_user_all_bypass() {
        let img = img();
        let h = addr();
        let supers = {
            let mut s = HashSet::new();
            s.insert("admin".to_string());
            s.insert("ops-bot".to_string());
            s
        };
        let admin = Principal {
            name: "admin".into(),
            auth_method: crabka_security::AuthMethod::SaslPlain,
            groups: vec![],
        };
        let ops = Principal {
            name: "ops-bot".into(),
            auth_method: crabka_security::AuthMethod::SaslPlain,
            groups: vec![],
        };
        let alice = alice();
        let auth = SimpleAclAuthorizer::new(supers);
        assert!(
            auth.authorize(&img, &req(&admin, &h, "foo", AclOperation::Write))
                == AuthorizationResult::Allow
        );
        assert!(
            auth.authorize(&img, &req(&ops, &h, "foo", AclOperation::Write))
                == AuthorizationResult::Allow
        );
        // alice is not a super-user and the image has no matching ACL,
        // so default-deny applies (no compat shim).
        assert!(
            auth.authorize(&img, &req(&alice, &h, "foo", AclOperation::Write))
                == AuthorizationResult::Deny
        );
    }
}
