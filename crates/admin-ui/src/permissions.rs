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
    let mut capabilities = Capabilities::default();
    let mut denied_capabilities = Capabilities::default();

    for entry in entries {
        if !is_for_principal(entry, principal) {
            continue;
        }

        match entry.permission_type {
            PermissionType::Allow => apply_entry(&mut capabilities, entry),
            PermissionType::Deny => apply_entry(&mut denied_capabilities, entry),
        }
    }

    remove_denied_capabilities(&mut capabilities, denied_capabilities);

    capabilities
}

fn is_for_principal(entry: &AclEntry, principal: &str) -> bool {
    entry.principal == principal || entry.principal == "User:*"
}

fn apply_entry(capabilities: &mut Capabilities, entry: &AclEntry) {
    match entry.resource_type {
        ResourceType::Topic => apply_topic_operation(capabilities, entry.operation),
        ResourceType::Group => apply_group_operation(capabilities, entry.operation),
        ResourceType::Cluster => apply_cluster_operation(capabilities, entry.operation),
        ResourceType::TransactionalId => {}
    }
}

fn remove_denied_capabilities(capabilities: &mut Capabilities, denied: Capabilities) {
    capabilities.can_view_topics &= !denied.can_view_topics;
    capabilities.can_create_topics &= !denied.can_create_topics;
    capabilities.can_alter_topics &= !denied.can_alter_topics;
    capabilities.can_delete_topics &= !denied.can_delete_topics;
    capabilities.can_view_groups &= !denied.can_view_groups;
    capabilities.can_view_acls &= !denied.can_view_acls;
    capabilities.can_alter_acls &= !denied.can_alter_acls;
    capabilities.can_alter_users &= !denied.can_alter_users;
    capabilities.can_view_quotas &= !denied.can_view_quotas;
    capabilities.can_alter_quotas &= !denied.can_alter_quotas;
    capabilities.can_view_log_dirs &= !denied.can_view_log_dirs;
}

fn apply_topic_operation(capabilities: &mut Capabilities, operation: AclOperation) {
    match operation {
        AclOperation::All => {
            capabilities.can_view_topics = true;
            capabilities.can_create_topics = true;
            capabilities.can_alter_topics = true;
            capabilities.can_delete_topics = true;
        }
        AclOperation::Describe | AclOperation::Read | AclOperation::Write => {
            capabilities.can_view_topics = true;
        }
        AclOperation::Alter | AclOperation::AlterConfigs => {
            capabilities.can_view_topics = true;
            capabilities.can_alter_topics = true;
        }
        AclOperation::Delete => {
            capabilities.can_view_topics = true;
            capabilities.can_delete_topics = true;
        }
        AclOperation::Create
        | AclOperation::ClusterAction
        | AclOperation::DescribeConfigs
        | AclOperation::IdempotentWrite
        | AclOperation::TwoPhaseCommit => {}
    }
}

fn apply_group_operation(capabilities: &mut Capabilities, operation: AclOperation) {
    match operation {
        AclOperation::All
        | AclOperation::Read
        | AclOperation::Delete
        | AclOperation::Alter
        | AclOperation::Describe => {
            capabilities.can_view_groups = true;
        }
        AclOperation::Write
        | AclOperation::Create
        | AclOperation::ClusterAction
        | AclOperation::DescribeConfigs
        | AclOperation::AlterConfigs
        | AclOperation::IdempotentWrite
        | AclOperation::TwoPhaseCommit => {}
    }
}

fn apply_cluster_operation(capabilities: &mut Capabilities, operation: AclOperation) {
    match operation {
        AclOperation::All => {
            capabilities.can_view_acls = true;
            capabilities.can_alter_acls = true;
            capabilities.can_alter_users = true;
            capabilities.can_view_quotas = true;
            capabilities.can_alter_quotas = true;
            capabilities.can_view_log_dirs = true;
        }
        AclOperation::Describe => {
            capabilities.can_view_acls = true;
            capabilities.can_view_log_dirs = true;
            capabilities.can_view_quotas = true;
        }
        AclOperation::Alter => {
            capabilities.can_view_acls = true;
            capabilities.can_view_log_dirs = true;
            capabilities.can_view_quotas = true;
            capabilities.can_alter_acls = true;
            capabilities.can_alter_users = true;
            capabilities.can_alter_quotas = true;
        }
        AclOperation::Create => capabilities.can_create_topics = true,
        AclOperation::DescribeConfigs
        | AclOperation::AlterConfigs
        | AclOperation::Read
        | AclOperation::Write
        | AclOperation::Delete
        | AclOperation::ClusterAction
        | AclOperation::IdempotentWrite
        | AclOperation::TwoPhaseCommit => {}
    }
}
