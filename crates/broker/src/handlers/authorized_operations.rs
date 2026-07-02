//! KIP-430. Compute the `(cluster|topic|group)_authorized_operations`
//! bitfield surfaced on `Metadata`, `DescribeCluster`, and `DescribeGroups`
//! responses when the corresponding request flag is set.
//!
//! Encoding: for each operation in the resource type's supported set,
//! ask the authorizer; OR `1 << op.code()` into the bitfield on Allow.
//! `op.code()` is the same wire discriminant the ACL handlers serialize
//! (see [`super::acl_wire::operation_to_wire`]).
//!
//! Kafka's convention: when the include flag is *not* set, the field is
//! `i32::MIN` (the "not present" sentinel). That's the schema-level
//! default already; handlers only populate the field when the request
//! opts in.
//!
//! Resource → supported-operation set tracks
//! `org.apache.kafka.common.acl.AclEntry#supportedOperations` (Kafka 3.6+):
//!
//! | resource         | operations                                                        |
//! |------------------|-------------------------------------------------------------------|
//! | Topic            | Read, Write, Create, Delete, Alter, Describe, DescribeConfigs,    |
//! |                  | AlterConfigs                                                      |
//! | Group            | Read, Describe, Delete                                            |
//! | Cluster          | Create, Alter, Describe, ClusterAction, AlterConfigs,             |
//! |                  | DescribeConfigs, IdempotentWrite                                  |
//! | TransactionalId  | Describe, Write, TwoPhaseCommit                                    |
//! | DelegationToken  | Describe                                                          |

use std::net::SocketAddr;

use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
use crabka_security::Principal;

use super::acl_wire::operation_to_wire;
use crate::authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer};

/// Returns the operations whose Allow decision contributes to the
/// authorized-operations bitfield for `resource_type`. Matches Kafka's
/// `AclEntry.supportedOperations(...)`.
#[must_use]
pub fn supported_operations(resource_type: ResourceType) -> &'static [AclOperation] {
    match resource_type {
        ResourceType::Topic => &[
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ],
        ResourceType::Group => &[
            AclOperation::Read,
            AclOperation::Describe,
            AclOperation::Delete,
        ],
        ResourceType::Cluster => &[
            AclOperation::Create,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::AlterConfigs,
            AclOperation::DescribeConfigs,
            AclOperation::IdempotentWrite,
        ],
        ResourceType::TransactionalId => &[
            AclOperation::Describe,
            AclOperation::Write,
            // KIP-939: 2PC participation is a grantable TransactionalId
            // permission, so it surfaces in the authorized-operations bitfield.
            AclOperation::TwoPhaseCommit,
        ],
        ResourceType::DelegationToken => &[AclOperation::Describe],
    }
}

/// Compute the authorized-operations bitfield for `(resource_type, resource_name)`
/// from `principal@host`'s perspective. The bit set for an operation is
/// `1 << operation_to_wire(op)`, matching Kafka's
/// `AuthorizationHelper.authorizedOperations(...)`.
#[must_use]
pub fn authorized_operations_bits(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    principal: &Principal,
    host: &SocketAddr,
    resource_type: ResourceType,
    resource_name: &str,
) -> i32 {
    let mut bits: i32 = 0;
    for &op in supported_operations(resource_type) {
        let allow = authorizer.authorize(
            image,
            &AuthorizationRequest {
                principal,
                host,
                resource_type,
                resource_name,
                operation: op,
            },
        );
        if allow == AuthorizationResult::Allow {
            bits |= 1_i32 << operation_to_wire(op);
        }
    }
    bits
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use std::collections::HashSet;

    use crabka_metadata::{AclEntry, MetadataRecord, PatternType, PermissionType, ResourceType};
    use crabka_security::{AuthMethod, Principal};
    use uuid::Uuid;

    use super::*;
    use crate::authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer};

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.into(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec![],
        }
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    fn allow_acl(rt: ResourceType, op: AclOperation, name: &str, user: &str) -> AclEntry {
        AclEntry {
            resource_type: rt,
            resource_name: name.into(),
            pattern_type: PatternType::Literal,
            principal: format!("User:{user}"),
            host: "*".into(),
            operation: op,
            permission_type: PermissionType::Allow,
        }
    }

    fn bit(op: AclOperation) -> i32 {
        1_i32 << operation_to_wire(op)
    }

    #[test]
    fn supported_operations_topic_matches_kafka() {
        let ops = supported_operations(ResourceType::Topic);
        // Order doesn't matter for callers but the set must match.
        let got: HashSet<_> = ops.iter().copied().collect();
        let want: HashSet<_> = [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ]
        .into_iter()
        .collect();
        assert!(got == want);
    }

    #[test]
    fn supported_operations_group_matches_kafka() {
        let got: HashSet<_> = supported_operations(ResourceType::Group)
            .iter()
            .copied()
            .collect();
        let want: HashSet<_> = [
            AclOperation::Read,
            AclOperation::Describe,
            AclOperation::Delete,
        ]
        .into_iter()
        .collect();
        assert!(got == want);
    }

    #[test]
    fn supported_operations_cluster_matches_kafka() {
        let got: HashSet<_> = supported_operations(ResourceType::Cluster)
            .iter()
            .copied()
            .collect();
        let want: HashSet<_> = [
            AclOperation::Create,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::AlterConfigs,
            AclOperation::DescribeConfigs,
            AclOperation::IdempotentWrite,
        ]
        .into_iter()
        .collect();
        assert!(got == want);
    }

    #[test]
    fn allow_all_authorizer_sets_every_supported_bit_for_each_resource() {
        let auth = AllowAllAuthorizer;
        let img = MetadataImage::new(Uuid::nil());
        let p = principal("anyone");
        let h = addr();

        for rt in [
            ResourceType::Topic,
            ResourceType::Group,
            ResourceType::Cluster,
            ResourceType::TransactionalId,
            ResourceType::DelegationToken,
        ] {
            let bits = authorized_operations_bits(&auth, &img, &p, &h, rt, "name");
            let expected = supported_operations(rt)
                .iter()
                .copied()
                .fold(0_i32, |acc, op| acc | bit(op));
            assert!(bits == expected, "{rt:?}: full mask under AllowAll");
        }
    }

    #[test]
    fn simple_acl_with_no_acls_yields_zero() {
        let mut supers = HashSet::new();
        supers.insert("ignored".to_string());
        let auth = SimpleAclAuthorizer::new(supers);
        let img = MetadataImage::new(Uuid::nil());
        let p = principal("alice");
        let h = addr();
        // alice is not a super-user and the image has no ACLs → every
        // supported op denies → bitfield is 0.
        let bits = authorized_operations_bits(&auth, &img, &p, &h, ResourceType::Topic, "foo");
        assert!(bits == 0);
    }

    #[test]
    fn super_user_gets_full_mask_per_resource() {
        let mut supers = HashSet::new();
        supers.insert("admin".to_string());
        let auth = SimpleAclAuthorizer::new(supers);
        let img = MetadataImage::new(Uuid::nil());
        let p = principal("admin");
        let h = addr();

        let topic_bits =
            authorized_operations_bits(&auth, &img, &p, &h, ResourceType::Topic, "foo");
        let topic_want = supported_operations(ResourceType::Topic)
            .iter()
            .copied()
            .fold(0_i32, |acc, op| acc | bit(op));
        assert!(topic_bits == topic_want);

        let group_bits = authorized_operations_bits(&auth, &img, &p, &h, ResourceType::Group, "g");
        let group_want = supported_operations(ResourceType::Group)
            .iter()
            .copied()
            .fold(0_i32, |acc, op| acc | bit(op));
        assert!(group_bits == group_want);
    }

    #[test]
    fn read_allow_on_topic_sets_read_and_describe_bits_only() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1AccessControlEntry(allow_acl(
            ResourceType::Topic,
            AclOperation::Read,
            "foo",
            "alice",
        )));
        let auth = SimpleAclAuthorizer::new(HashSet::new());
        let p = principal("alice");
        let h = addr();
        let bits = authorized_operations_bits(&auth, &img, &p, &h, ResourceType::Topic, "foo");
        // Read ACL grants Read directly and Describe via implication.
        // No other supported op should be set.
        let expected = bit(AclOperation::Read) | bit(AclOperation::Describe);
        assert!(bits == expected);
    }

    #[test]
    fn write_allow_on_topic_sets_write_and_describe_only() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1AccessControlEntry(allow_acl(
            ResourceType::Topic,
            AclOperation::Write,
            "foo",
            "alice",
        )));
        let auth = SimpleAclAuthorizer::new(HashSet::new());
        let p = principal("alice");
        let h = addr();
        let bits = authorized_operations_bits(&auth, &img, &p, &h, ResourceType::Topic, "foo");
        let expected = bit(AclOperation::Write) | bit(AclOperation::Describe);
        assert!(bits == expected);
    }

    #[test]
    fn read_allow_on_group_sets_read_and_describe_only() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1AccessControlEntry(allow_acl(
            ResourceType::Group,
            AclOperation::Read,
            "cg",
            "alice",
        )));
        let auth = SimpleAclAuthorizer::new(HashSet::new());
        let p = principal("alice");
        let h = addr();
        let bits = authorized_operations_bits(&auth, &img, &p, &h, ResourceType::Group, "cg");
        let expected = bit(AclOperation::Read) | bit(AclOperation::Describe);
        assert!(bits == expected);
        // Bit for Delete (6) must NOT be set.
        assert!(bits & bit(AclOperation::Delete) == 0);
    }

    #[test]
    fn deny_wins_over_allow_in_bitfield() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1AccessControlEntry(allow_acl(
            ResourceType::Topic,
            AclOperation::Read,
            "foo",
            "alice",
        )));
        img.apply(&MetadataRecord::V1AccessControlEntry(AclEntry {
            permission_type: PermissionType::Deny,
            ..allow_acl(ResourceType::Topic, AclOperation::Read, "foo", "alice")
        }));
        let auth = SimpleAclAuthorizer::new(HashSet::new());
        let p = principal("alice");
        let h = addr();
        let bits = authorized_operations_bits(&auth, &img, &p, &h, ResourceType::Topic, "foo");
        // Read is denied; the Describe-via-Read implication also collapses
        // because the matching ACL row that would have granted it now
        // resolves to Deny under matches_operation. Bitfield is 0.
        assert!(bits == 0);
    }

    #[test]
    fn bit_values_match_kafka_int8_codes() {
        // Sanity: spot-check that the bit positions equal Kafka's wire
        // discriminants. If `operation_to_wire` ever drifts from
        // Kafka's `AclOperation.code()`, the wire field would become
        // unintelligible to JVM clients.
        for (op, want) in [
            (AclOperation::Read, 1 << 3),
            (AclOperation::Write, 1 << 4),
            (AclOperation::Create, 1 << 5),
            (AclOperation::Delete, 1 << 6),
            (AclOperation::Alter, 1 << 7),
            (AclOperation::Describe, 1 << 8),
            (AclOperation::ClusterAction, 1 << 9),
            (AclOperation::DescribeConfigs, 1 << 10),
            (AclOperation::AlterConfigs, 1 << 11),
            (AclOperation::IdempotentWrite, 1 << 12),
        ] {
            assert!(bit(op) == want, "{op:?}");
        }
    }
}
