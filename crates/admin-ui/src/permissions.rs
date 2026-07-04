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

    for entry in entries {
        if !is_allow_for_principal(entry, principal) {
            continue;
        }

        apply_allowed_entry(&mut capabilities, entry);
    }

    capabilities
}

fn is_allow_for_principal(entry: &AclEntry, principal: &str) -> bool {
    entry.permission_type == PermissionType::Allow && entry.principal == principal
}

fn apply_allowed_entry(capabilities: &mut Capabilities, entry: &AclEntry) {
    match entry.resource_type {
        ResourceType::Topic => apply_topic_operation(capabilities, entry.operation),
        ResourceType::Group => apply_group_operation(capabilities, entry.operation),
        ResourceType::Cluster => apply_cluster_operation(capabilities, entry.operation),
        ResourceType::TransactionalId => {}
    }
}

fn apply_topic_operation(capabilities: &mut Capabilities, operation: AclOperation) {
    match operation {
        AclOperation::All => {
            capabilities.can_view_topics = true;
            capabilities.can_create_topics = true;
            capabilities.can_alter_topics = true;
            capabilities.can_delete_topics = true;
        }
        AclOperation::Describe | AclOperation::Read => capabilities.can_view_topics = true,
        AclOperation::Create => capabilities.can_create_topics = true,
        AclOperation::Alter | AclOperation::AlterConfigs => capabilities.can_alter_topics = true,
        AclOperation::Delete => capabilities.can_delete_topics = true,
        AclOperation::Write
        | AclOperation::ClusterAction
        | AclOperation::DescribeConfigs
        | AclOperation::IdempotentWrite
        | AclOperation::TwoPhaseCommit => {}
    }
}

fn apply_group_operation(capabilities: &mut Capabilities, operation: AclOperation) {
    match operation {
        AclOperation::All | AclOperation::Describe | AclOperation::Read => {
            capabilities.can_view_groups = true;
        }
        AclOperation::Write
        | AclOperation::Create
        | AclOperation::Delete
        | AclOperation::Alter
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
            capabilities.can_view_log_dirs = true;
            capabilities.can_view_quotas = true;
        }
        AclOperation::DescribeConfigs => capabilities.can_view_quotas = true,
        AclOperation::Alter => {
            capabilities.can_alter_acls = true;
            capabilities.can_alter_users = true;
            capabilities.can_alter_quotas = true;
        }
        AclOperation::AlterConfigs => capabilities.can_alter_quotas = true,
        AclOperation::Read
        | AclOperation::Write
        | AclOperation::Create
        | AclOperation::Delete
        | AclOperation::ClusterAction
        | AclOperation::IdempotentWrite
        | AclOperation::TwoPhaseCommit => {}
    }
}
