use crabka_client_admin::{AclEntry, AclOperation, PermissionType, ResourceType};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Capabilities(u16);

impl Capabilities {
    const VIEW_TOPICS: u16 = 1 << 0;
    const CREATE_TOPICS: u16 = 1 << 1;
    const ALTER_TOPICS: u16 = 1 << 2;
    const DELETE_TOPICS: u16 = 1 << 3;
    const VIEW_GROUPS: u16 = 1 << 4;
    const VIEW_ACLS: u16 = 1 << 5;
    const ALTER_ACLS: u16 = 1 << 6;
    const ALTER_USERS: u16 = 1 << 7;
    const VIEW_QUOTAS: u16 = 1 << 8;
    const ALTER_QUOTAS: u16 = 1 << 9;
    const VIEW_LOG_DIRS: u16 = 1 << 10;

    const fn contains(self, capability: u16) -> bool {
        self.0 & capability != 0
    }

    #[must_use]
    pub const fn can_view_topics(self) -> bool {
        self.contains(Self::VIEW_TOPICS)
    }
    #[must_use]
    pub const fn can_create_topics(self) -> bool {
        self.contains(Self::CREATE_TOPICS)
    }
    #[must_use]
    pub const fn can_alter_topics(self) -> bool {
        self.contains(Self::ALTER_TOPICS)
    }
    #[must_use]
    pub const fn can_delete_topics(self) -> bool {
        self.contains(Self::DELETE_TOPICS)
    }
    #[must_use]
    pub const fn can_view_groups(self) -> bool {
        self.contains(Self::VIEW_GROUPS)
    }
    #[must_use]
    pub const fn can_view_acls(self) -> bool {
        self.contains(Self::VIEW_ACLS)
    }
    #[must_use]
    pub const fn can_alter_acls(self) -> bool {
        self.contains(Self::ALTER_ACLS)
    }
    #[must_use]
    pub const fn can_alter_users(self) -> bool {
        self.contains(Self::ALTER_USERS)
    }
    #[must_use]
    pub const fn can_view_quotas(self) -> bool {
        self.contains(Self::VIEW_QUOTAS)
    }
    #[must_use]
    pub const fn can_alter_quotas(self) -> bool {
        self.contains(Self::ALTER_QUOTAS)
    }
    #[must_use]
    pub const fn can_view_log_dirs(self) -> bool {
        self.contains(Self::VIEW_LOG_DIRS)
    }
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

    let mut bits = 0;
    let mut grant = |capability, granted| {
        if granted {
            bits |= capability;
        }
    };
    grant(
        Capabilities::VIEW_TOPICS,
        effective(ResourceType::Topic, AclOperation::Describe),
    );
    grant(
        Capabilities::CREATE_TOPICS,
        effective(ResourceType::Cluster, AclOperation::Create),
    );
    grant(
        Capabilities::ALTER_TOPICS,
        effective(ResourceType::Topic, AclOperation::Alter)
            || effective(ResourceType::Topic, AclOperation::AlterConfigs),
    );
    grant(
        Capabilities::DELETE_TOPICS,
        effective(ResourceType::Topic, AclOperation::Delete),
    );
    grant(
        Capabilities::VIEW_GROUPS,
        effective(ResourceType::Group, AclOperation::Describe),
    );
    grant(
        Capabilities::VIEW_ACLS,
        effective(ResourceType::Cluster, AclOperation::Describe),
    );
    grant(
        Capabilities::ALTER_ACLS,
        effective(ResourceType::Cluster, AclOperation::Alter),
    );
    grant(
        Capabilities::ALTER_USERS,
        effective(ResourceType::Cluster, AclOperation::Alter),
    );
    grant(
        Capabilities::VIEW_QUOTAS,
        effective(ResourceType::Cluster, AclOperation::Describe),
    );
    grant(
        Capabilities::ALTER_QUOTAS,
        effective(ResourceType::Cluster, AclOperation::Alter),
    );
    grant(
        Capabilities::VIEW_LOG_DIRS,
        effective(ResourceType::Cluster, AclOperation::Describe),
    );
    Capabilities(bits)
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
