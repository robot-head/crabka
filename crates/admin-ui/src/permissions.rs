use crabka_client_admin::{AclEntry, AclOperation, PermissionType, ResourceType};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct Capabilities {
    pub can_view_topics: bool,
    pub can_create_topics: bool,
    pub can_alter_topics: bool,
    pub can_delete_topics: bool,
    pub can_view_groups: bool,
    pub can_view_acls: bool,
    pub can_alter_acls: bool,
    pub can_alter_users: bool,
    pub can_view_quotas: bool,
    pub can_alter_quotas: bool,
    pub can_view_log_dirs: bool,
}

#[must_use]
pub fn derive_capabilities(principal: &str, entries: &[AclEntry]) -> Capabilities {
    derive_capabilities_for_optional_host(principal, None, entries)
}

#[must_use]
pub fn derive_capabilities_for_host(
    principal: &str,
    peer_host: &str,
    entries: &[AclEntry],
) -> Capabilities {
    derive_capabilities_for_optional_host(principal, Some(peer_host), entries)
}

fn derive_capabilities_for_optional_host(
    principal: &str,
    peer_host: Option<&str>,
    entries: &[AclEntry],
) -> Capabilities {
    let effective = |resource_type, operation| {
        has_effective_permission(principal, peer_host, entries, resource_type, operation)
    };

    Capabilities {
        can_view_topics: effective(ResourceType::Topic, AclOperation::Describe),
        can_create_topics: effective(ResourceType::Cluster, AclOperation::Create),
        can_alter_topics: effective(ResourceType::Topic, AclOperation::Alter)
            || effective(ResourceType::Topic, AclOperation::AlterConfigs),
        can_delete_topics: effective(ResourceType::Topic, AclOperation::Delete),
        can_view_groups: effective(ResourceType::Group, AclOperation::Describe),
        can_view_acls: effective(ResourceType::Cluster, AclOperation::Describe),
        can_alter_acls: effective(ResourceType::Cluster, AclOperation::Alter),
        can_alter_users: effective(ResourceType::Cluster, AclOperation::Alter),
        can_view_quotas: effective(ResourceType::Cluster, AclOperation::Describe),
        can_alter_quotas: effective(ResourceType::Cluster, AclOperation::Alter),
        can_view_log_dirs: effective(ResourceType::Cluster, AclOperation::Describe),
    }
}

fn is_for_principal_and_host(entry: &AclEntry, principal: &str, peer_host: Option<&str>) -> bool {
    is_for_principal(entry, principal) && is_for_host(entry, peer_host)
}

fn is_for_principal(entry: &AclEntry, principal: &str) -> bool {
    entry.principal == principal || entry.principal == "User:*"
}

fn is_for_host(entry: &AclEntry, peer_host: Option<&str>) -> bool {
    if entry.host == "*" {
        return true;
    }

    peer_host.is_some_and(|host| entry.host == host)
}

fn has_effective_permission(
    principal: &str,
    peer_host: Option<&str>,
    entries: &[AclEntry],
    resource_type: ResourceType,
    operation: AclOperation,
) -> bool {
    let mut saw_allow = false;

    for entry in entries {
        if entry.resource_type != resource_type
            || !is_for_principal_and_host(entry, principal, peer_host)
            || !matches_operation(entry.operation, operation)
        {
            continue;
        }

        match entry.permission_type {
            PermissionType::Allow => saw_allow = true,
            PermissionType::Deny => return false,
        }
    }

    saw_allow
}

fn matches_operation(stored: AclOperation, requested: AclOperation) -> bool {
    stored == requested || matches!(stored, AclOperation::All) || implies(stored, requested)
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
