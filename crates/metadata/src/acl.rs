//! Wire-stable ACL types replicated through the raft quorum
//! via `MetadataRecord::V1AccessControlEntry` /
//! `V1DeleteAccessControlEntry`. Mirrors the shape Kafka exposes on the
//! `CreateAcls` / `DeleteAcls` / `DescribeAcls` wire messages, but stays
//! pure data — the authorizer in `crabka-broker` evaluates these.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    /// KIP-48 delegation tokens. Resource name is the owner's
    /// `KafkaPrincipal` string form (e.g. `"User:alice"`). Pattern types
    /// `LITERAL`/`PREFIXED` apply via the existing matcher; only the
    /// `Describe` operation is externally grantable
    /// (`Create`/`Renew`/`Expire` are implicit on ownership).
    DelegationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PatternType {
    Literal,
    Prefixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PermissionType {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AclOperation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
    /// KIP-939: permission to participate in two-phase commit (2PC) on a
    /// `TransactionalId`. Required (in addition to `Write`) for an
    /// `InitProducerId` carrying `enable2Pc=true`. Kafka wire discriminant 15.
    TwoPhaseCommit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclEntry {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub pattern_type: PatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: PermissionType,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclEntryFilter {
    pub resource_type: Option<ResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<PatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<AclOperation>,
    pub permission_type: Option<PermissionType>,
}

impl AclEntryFilter {
    /// Returns true if every populated axis matches `entry`. `None` axes
    /// match anything.
    #[must_use]
    pub fn matches(&self, entry: &AclEntry) -> bool {
        self.resource_type
            .is_none_or(|rt| rt == entry.resource_type)
            && self
                .resource_name
                .as_ref()
                .is_none_or(|rn| rn == &entry.resource_name)
            && self.pattern_type.is_none_or(|pt| pt == entry.pattern_type)
            && self
                .principal
                .as_ref()
                .is_none_or(|p| p == &entry.principal)
            && self.host.as_ref().is_none_or(|h| h == &entry.host)
            && self.operation.is_none_or(|op| op == entry.operation)
            && self
                .permission_type
                .is_none_or(|pt| pt == entry.permission_type)
    }
}

#[cfg(test)]
mod tests {

    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};

    use super::*;

    fn rt<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug,
    {
        let bytes = <SerdeCompat<T>>::serialize(value).unwrap();
        <SerdeCompat<T>>::deserialize(&bytes).unwrap()
    }

    #[test]
    fn acl_entry_round_trip() {
        let entry = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "foo".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        assert2::assert!(rt(&entry) == entry);
    }

    #[test]
    fn acl_entry_filter_round_trip() {
        let filter = AclEntryFilter {
            resource_type: Some(ResourceType::Group),
            resource_name: None,
            pattern_type: Some(PatternType::Prefixed),
            principal: Some("User:bob".into()),
            host: None,
            operation: Some(AclOperation::All),
            permission_type: None,
        };
        assert2::assert!(rt(&filter) == filter);
    }

    #[test]
    fn filter_with_all_none_matches_anything() {
        let f = AclEntryFilter::default();
        let entry = AclEntry {
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster".into(),
            pattern_type: PatternType::Literal,
            principal: "User:admin".into(),
            host: "*".into(),
            operation: AclOperation::All,
            permission_type: PermissionType::Allow,
        };
        assert2::assert!(f.matches(&entry));
    }

    #[test]
    fn filter_with_specific_axis_filters_correctly() {
        let f = AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            ..AclEntryFilter::default()
        };
        let topic_entry = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "foo".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        let group_entry = AclEntry {
            resource_type: ResourceType::Group,
            ..topic_entry.clone()
        };
        assert2::assert!((f.matches(&topic_entry), f.matches(&group_entry)) == (true, false));
    }
}
