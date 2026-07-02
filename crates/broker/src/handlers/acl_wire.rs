//! Wire ↔ metadata enum mapping for ACL handlers.
//!
//! Kafka serializes ACL enums as `i8` discriminants. This module
//! provides the conversions and a tiny error type for "unknown
//! discriminant" / "ANY where a concrete value is required" cases.

use crabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};

/// Kafka's singleton cluster resource name (`Resource.CLUSTER_NAME`);
/// every cluster-scoped ACL and authorization check targets this name.
pub const CLUSTER_RESOURCE_NAME: &str = "kafka-cluster";

/// Wire `i8` discriminant of an ACL `resource_type` field.
pub type ResourceTypeCode = i8;
/// Wire `i8` discriminant of an ACL `pattern_type` field.
pub type PatternTypeCode = i8;
/// Wire `i8` discriminant of an ACL `operation` field.
pub type OperationCode = i8;
/// Wire `i8` discriminant of an ACL `permission_type` field.
pub type PermissionTypeCode = i8;

/// Wire byte for `UNKNOWN` (0) — shared by every ACL enum axis.
const WIRE_UNKNOWN: i8 = 0;
/// Wire byte for `ANY` (1) — a filter wildcard, never a concrete value;
/// shared by every ACL enum axis.
const WIRE_ANY: i8 = 1;
/// Wire byte for `PatternType::MATCH` (2, KIP-290) — a filter-only
/// wildcard that matches both literal and prefixed patterns.
const WIRE_PATTERN_MATCH: i8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireAclError {
    UnknownDiscriminant,
    /// ANY/MATCH was used where a concrete value is required.
    AnyRequiresFilter,
}

/// Parse a wire `resource_type` byte into a concrete `ResourceType`.
/// Used by `CreateAcls` where the resource must be concrete.
pub fn resource_type_concrete(b: ResourceTypeCode) -> Result<ResourceType, WireAclError> {
    match b {
        2 => Ok(ResourceType::Topic),
        3 => Ok(ResourceType::Group),
        4 => Ok(ResourceType::Cluster),
        5 => Ok(ResourceType::TransactionalId),
        6 => Ok(ResourceType::DelegationToken),
        // 7 (User) / 0 (Unknown) / 1 (Any) rejected.
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Parse a wire `resource_type` byte into a filter slot.
///
/// `Any` (1) maps to `None`. Used by `DeleteAcls` and `DescribeAcls`.
pub fn resource_type_filter(b: ResourceTypeCode) -> Result<Option<ResourceType>, WireAclError> {
    match b {
        WIRE_ANY => Ok(None),
        2 => Ok(Some(ResourceType::Topic)),
        3 => Ok(Some(ResourceType::Group)),
        4 => Ok(Some(ResourceType::Cluster)),
        5 => Ok(Some(ResourceType::TransactionalId)),
        6 => Ok(Some(ResourceType::DelegationToken)),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Encode a `ResourceType` as its wire `i8` discriminant.
#[must_use]
pub fn resource_type_to_wire(rt: ResourceType) -> ResourceTypeCode {
    match rt {
        ResourceType::Topic => 2,
        ResourceType::Group => 3,
        ResourceType::Cluster => 4,
        ResourceType::TransactionalId => 5,
        ResourceType::DelegationToken => 6,
    }
}

/// Parse a wire `pattern_type` byte into a concrete `PatternType`.
/// Used by `CreateAcls` where the pattern must be concrete.
pub fn pattern_type_concrete(b: PatternTypeCode) -> Result<PatternType, WireAclError> {
    match b {
        3 => Ok(PatternType::Literal),
        4 => Ok(PatternType::Prefixed),
        WIRE_UNKNOWN..=WIRE_PATTERN_MATCH => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Parse a wire `pattern_type` byte into a filter slot.
///
/// `Any` (1) and `Match` (2) both collapse to `None`. Used by
/// `DeleteAcls` and `DescribeAcls`.
pub fn pattern_type_filter(b: PatternTypeCode) -> Result<Option<PatternType>, WireAclError> {
    match b {
        // ANY / MATCH both collapse to None for our matcher.
        WIRE_ANY | WIRE_PATTERN_MATCH => Ok(None),
        3 => Ok(Some(PatternType::Literal)),
        4 => Ok(Some(PatternType::Prefixed)),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Encode a `PatternType` as its wire `i8` discriminant.
#[must_use]
pub fn pattern_type_to_wire(pt: PatternType) -> PatternTypeCode {
    match pt {
        PatternType::Literal => 3,
        PatternType::Prefixed => 4,
    }
}

/// Parse a wire `operation` byte into a concrete `AclOperation`.
/// Used by `CreateAcls` where the operation must be concrete.
pub fn operation_concrete(b: OperationCode) -> Result<AclOperation, WireAclError> {
    match b {
        2 => Ok(AclOperation::All),
        3 => Ok(AclOperation::Read),
        4 => Ok(AclOperation::Write),
        5 => Ok(AclOperation::Create),
        6 => Ok(AclOperation::Delete),
        7 => Ok(AclOperation::Alter),
        8 => Ok(AclOperation::Describe),
        9 => Ok(AclOperation::ClusterAction),
        10 => Ok(AclOperation::DescribeConfigs),
        11 => Ok(AclOperation::AlterConfigs),
        12 => Ok(AclOperation::IdempotentWrite),
        15 => Ok(AclOperation::TwoPhaseCommit),
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Parse a wire `operation` byte into a filter slot.
///
/// `Any` (1) maps to `None`. Used by `DeleteAcls` and `DescribeAcls`.
pub fn operation_filter(b: OperationCode) -> Result<Option<AclOperation>, WireAclError> {
    match b {
        WIRE_ANY => Ok(None),
        2 => Ok(Some(AclOperation::All)),
        3 => Ok(Some(AclOperation::Read)),
        4 => Ok(Some(AclOperation::Write)),
        5 => Ok(Some(AclOperation::Create)),
        6 => Ok(Some(AclOperation::Delete)),
        7 => Ok(Some(AclOperation::Alter)),
        8 => Ok(Some(AclOperation::Describe)),
        9 => Ok(Some(AclOperation::ClusterAction)),
        10 => Ok(Some(AclOperation::DescribeConfigs)),
        11 => Ok(Some(AclOperation::AlterConfigs)),
        12 => Ok(Some(AclOperation::IdempotentWrite)),
        15 => Ok(Some(AclOperation::TwoPhaseCommit)),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Encode an `AclOperation` as its wire `i8` discriminant.
#[must_use]
pub fn operation_to_wire(op: AclOperation) -> OperationCode {
    match op {
        AclOperation::All => 2,
        AclOperation::Read => 3,
        AclOperation::Write => 4,
        AclOperation::Create => 5,
        AclOperation::Delete => 6,
        AclOperation::Alter => 7,
        AclOperation::Describe => 8,
        AclOperation::ClusterAction => 9,
        AclOperation::DescribeConfigs => 10,
        AclOperation::AlterConfigs => 11,
        AclOperation::IdempotentWrite => 12,
        AclOperation::TwoPhaseCommit => 15,
    }
}

/// Parse a wire `permission_type` byte into a concrete `PermissionType`.
/// Used by `CreateAcls` where the permission must be concrete.
pub fn permission_concrete(b: PermissionTypeCode) -> Result<PermissionType, WireAclError> {
    match b {
        2 => Ok(PermissionType::Deny),
        3 => Ok(PermissionType::Allow),
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Parse a wire `permission_type` byte into a filter slot.
///
/// `Any` (1) maps to `None`. Used by `DeleteAcls` and `DescribeAcls`.
pub fn permission_filter(b: PermissionTypeCode) -> Result<Option<PermissionType>, WireAclError> {
    match b {
        WIRE_ANY => Ok(None),
        2 => Ok(Some(PermissionType::Deny)),
        3 => Ok(Some(PermissionType::Allow)),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Encode a `PermissionType` as its wire `i8` discriminant.
#[must_use]
pub fn permission_to_wire(pt: PermissionType) -> PermissionTypeCode {
    match pt {
        PermissionType::Deny => 2,
        PermissionType::Allow => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::{assert, check};

    #[test]
    fn resource_type_concrete_rejects_any() {
        let cases = [
            (1, Err(WireAclError::AnyRequiresFilter)),
            (2, Ok(ResourceType::Topic)),
            (3, Ok(ResourceType::Group)),
            (4, Ok(ResourceType::Cluster)),
        ];
        for (byte, want) in cases {
            assert!(resource_type_concrete(byte) == want, "byte {byte}");
        }
    }

    #[test]
    fn pattern_type_filter_any_and_match_collapse_to_none() {
        let cases = [
            (1, Ok(None)),
            (2, Ok(None)),
            (3, Ok(Some(PatternType::Literal))),
        ];
        for (byte, want) in cases {
            assert!(pattern_type_filter(byte) == want, "byte {byte}");
        }
    }

    #[test]
    fn resource_type_filter_collapses_any_and_rejects_unknown() {
        let cases = [
            (WIRE_ANY, Ok(None)),
            (2, Ok(Some(ResourceType::Topic))),
            (3, Ok(Some(ResourceType::Group))),
            (4, Ok(Some(ResourceType::Cluster))),
            (5, Ok(Some(ResourceType::TransactionalId))),
            (6, Ok(Some(ResourceType::DelegationToken))),
            (7, Err(WireAclError::UnknownDiscriminant)),
        ];
        for (byte, want) in cases {
            assert!(resource_type_filter(byte) == want, "byte {byte}");
        }
    }

    #[test]
    fn concrete_parsers_distinguish_wildcards_from_unknown_discriminants() {
        // The wildcard bytes (UNKNOWN/ANY, and MATCH for pattern types) must
        // report AnyRequiresFilter — a different wire error than a junk
        // discriminant — and the concrete codes must map, not fall through.
        let pattern_cases = [
            (3, Ok(PatternType::Literal)),
            (4, Ok(PatternType::Prefixed)),
            (WIRE_UNKNOWN, Err(WireAclError::AnyRequiresFilter)),
            (WIRE_ANY, Err(WireAclError::AnyRequiresFilter)),
            (WIRE_PATTERN_MATCH, Err(WireAclError::AnyRequiresFilter)),
            (5, Err(WireAclError::UnknownDiscriminant)),
        ];
        for (byte, want) in pattern_cases {
            check!(pattern_type_concrete(byte) == want, "pattern byte {byte}");
        }

        for byte in [WIRE_UNKNOWN, WIRE_ANY] {
            check!(
                operation_concrete(byte) == Err(WireAclError::AnyRequiresFilter),
                "operation byte {byte}"
            );
            check!(
                permission_concrete(byte) == Err(WireAclError::AnyRequiresFilter),
                "permission byte {byte}"
            );
        }
    }

    #[test]
    fn operation_round_trip_through_wire() {
        for op in [
            AclOperation::All,
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::IdempotentWrite,
            // KIP-939: TWO_PHASE_COMMIT, wire byte 15.
            AclOperation::TwoPhaseCommit,
        ] {
            let b = operation_to_wire(op);
            assert!(operation_concrete(b).unwrap() == op);
        }
    }

    #[test]
    fn permission_filter_maps_any_concrete_and_unknown() {
        let cases = [
            (WIRE_ANY, Ok(None)),
            (2, Ok(Some(PermissionType::Deny))),
            (3, Ok(Some(PermissionType::Allow))),
            (4, Err(WireAclError::UnknownDiscriminant)),
        ];
        for (byte, want) in cases {
            check!(permission_filter(byte) == want, "byte {byte}");
        }
    }

    /// KIP-939: the `TWO_PHASE_COMMIT` operation is wire byte 15 and must
    /// round-trip through the concrete + filter codecs so `CreateAcls` /
    /// `DescribeAcls` can carry the 2PC grant on a `TransactionalId`.
    #[test]
    fn two_phase_commit_operation_is_byte_15() {
        check!(operation_to_wire(AclOperation::TwoPhaseCommit) == 15);
        check!(operation_concrete(15) == Ok(AclOperation::TwoPhaseCommit));
        check!(operation_filter(15) == Ok(Some(AclOperation::TwoPhaseCommit)));
    }

    /// The KIP-48 `TOKEN` (a.k.a. `DELEGATION_TOKEN`)
    /// resource type, wire byte 6, must round-trip through the wire
    /// codec so `CreateAcls`/`DeleteAcls`/`DescribeAcls` can carry
    /// ACLs guarding delegation tokens.
    #[test]
    fn delegation_token_resource_type_now_accepted() {
        use crabka_metadata::AclEntry;

        // Concrete (CreateAcls) codec.
        check!(resource_type_concrete(6) == Ok(ResourceType::DelegationToken));
        // Filter (Delete/DescribeAcls) codec.
        check!(resource_type_filter(6) == Ok(Some(ResourceType::DelegationToken)));
        // Encoder.
        check!(resource_type_to_wire(ResourceType::DelegationToken) == 6);

        // Build a concrete AclEntry at the canonical (Describe, Allow)
        // shape KIP-48 token ACLs use; verify the wire bytes line up.
        let entry = AclEntry {
            resource_type: resource_type_concrete(6).unwrap(),
            resource_name: "User:alice".into(),
            pattern_type: PatternType::Literal,
            principal: "User:bob".into(),
            host: "*".into(),
            operation: operation_concrete(8).unwrap(),
            permission_type: permission_concrete(3).unwrap(),
        };
        assert!(resource_type_to_wire(entry.resource_type) == 6);
        let expected = AclEntry {
            resource_type: ResourceType::DelegationToken,
            resource_name: "User:alice".into(),
            pattern_type: PatternType::Literal,
            principal: "User:bob".into(),
            host: "*".into(),
            operation: AclOperation::Describe,
            permission_type: PermissionType::Allow,
        };
        assert!(entry == expected);
    }
}
