//! Slice 13. Wire ↔ metadata enum mapping for ACL handlers.
//!
//! Kafka serializes ACL enums as `i8` discriminants. This module
//! provides the conversions and a tiny error type for "unknown
//! discriminant" / "ANY where a concrete value is required" cases.

use crabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireAclError {
    UnknownDiscriminant,
    /// ANY/MATCH was used where a concrete value is required.
    AnyRequiresFilter,
}

/// Parse a wire `resource_type` byte into a concrete `ResourceType`.
/// Used by `CreateAcls` where the resource must be concrete.
pub fn resource_type_concrete(b: i8) -> Result<ResourceType, WireAclError> {
    match b {
        2 => Ok(ResourceType::Topic),
        3 => Ok(ResourceType::Group),
        4 => Ok(ResourceType::Cluster),
        5 => Ok(ResourceType::TransactionalId),
        // 6 (DelegationToken) / 7 (User) / 0 (Unknown) / 1 (Any) all rejected.
        0 | 1 => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Parse a wire `resource_type` byte into a filter slot.
///
/// `Any` (1) maps to `None`. Used by `DeleteAcls` and `DescribeAcls`.
pub fn resource_type_filter(b: i8) -> Result<Option<ResourceType>, WireAclError> {
    match b {
        1 => Ok(None),
        2 => Ok(Some(ResourceType::Topic)),
        3 => Ok(Some(ResourceType::Group)),
        4 => Ok(Some(ResourceType::Cluster)),
        5 => Ok(Some(ResourceType::TransactionalId)),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Encode a `ResourceType` as its wire `i8` discriminant.
#[must_use]
pub fn resource_type_to_wire(rt: ResourceType) -> i8 {
    match rt {
        ResourceType::Topic => 2,
        ResourceType::Group => 3,
        ResourceType::Cluster => 4,
        ResourceType::TransactionalId => 5,
    }
}

/// Parse a wire `pattern_type` byte into a concrete `PatternType`.
/// Used by `CreateAcls` where the pattern must be concrete.
pub fn pattern_type_concrete(b: i8) -> Result<PatternType, WireAclError> {
    match b {
        3 => Ok(PatternType::Literal),
        4 => Ok(PatternType::Prefixed),
        0..=2 => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Parse a wire `pattern_type` byte into a filter slot.
///
/// `Any` (1) and `Match` (2) both collapse to `None`. Used by
/// `DeleteAcls` and `DescribeAcls`.
pub fn pattern_type_filter(b: i8) -> Result<Option<PatternType>, WireAclError> {
    match b {
        1 | 2 => Ok(None), // ANY / MATCH both collapse to None for our matcher
        3 => Ok(Some(PatternType::Literal)),
        4 => Ok(Some(PatternType::Prefixed)),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Encode a `PatternType` as its wire `i8` discriminant.
#[must_use]
pub fn pattern_type_to_wire(pt: PatternType) -> i8 {
    match pt {
        PatternType::Literal => 3,
        PatternType::Prefixed => 4,
    }
}

/// Parse a wire `operation` byte into a concrete `AclOperation`.
/// Used by `CreateAcls` where the operation must be concrete.
pub fn operation_concrete(b: i8) -> Result<AclOperation, WireAclError> {
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
        0 | 1 => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Parse a wire `operation` byte into a filter slot.
///
/// `Any` (1) maps to `None`. Used by `DeleteAcls` and `DescribeAcls`.
pub fn operation_filter(b: i8) -> Result<Option<AclOperation>, WireAclError> {
    match b {
        1 => Ok(None),
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
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Encode an `AclOperation` as its wire `i8` discriminant.
#[must_use]
pub fn operation_to_wire(op: AclOperation) -> i8 {
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
    }
}

/// Parse a wire `permission_type` byte into a concrete `PermissionType`.
/// Used by `CreateAcls` where the permission must be concrete.
pub fn permission_concrete(b: i8) -> Result<PermissionType, WireAclError> {
    match b {
        2 => Ok(PermissionType::Deny),
        3 => Ok(PermissionType::Allow),
        0 | 1 => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Parse a wire `permission_type` byte into a filter slot.
///
/// `Any` (1) maps to `None`. Used by `DeleteAcls` and `DescribeAcls`.
pub fn permission_filter(b: i8) -> Result<Option<PermissionType>, WireAclError> {
    match b {
        1 => Ok(None),
        2 => Ok(Some(PermissionType::Deny)),
        3 => Ok(Some(PermissionType::Allow)),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

/// Encode a `PermissionType` as its wire `i8` discriminant.
#[must_use]
pub fn permission_to_wire(pt: PermissionType) -> i8 {
    match pt {
        PermissionType::Deny => 2,
        PermissionType::Allow => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_type_concrete_rejects_any() {
        assert_eq!(
            resource_type_concrete(1),
            Err(WireAclError::AnyRequiresFilter)
        );
        assert_eq!(resource_type_concrete(2), Ok(ResourceType::Topic));
    }

    #[test]
    fn pattern_type_filter_any_and_match_collapse_to_none() {
        assert_eq!(pattern_type_filter(1), Ok(None));
        assert_eq!(pattern_type_filter(2), Ok(None));
        assert_eq!(pattern_type_filter(3), Ok(Some(PatternType::Literal)));
    }

    #[test]
    fn operation_round_trip_through_wire() {
        for op in [
            AclOperation::All,
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::IdempotentWrite,
        ] {
            let b = operation_to_wire(op);
            assert_eq!(operation_concrete(b).unwrap(), op);
        }
    }
}
