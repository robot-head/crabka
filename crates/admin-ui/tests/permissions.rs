use crabka_admin_ui::permissions::{Capabilities, derive_capabilities};
use crabka_client_admin::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};

fn allow(resource_type: ResourceType, operation: AclOperation) -> AclEntry {
    AclEntry {
        resource_type,
        resource_name: "*".to_string(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".to_string(),
        host: "*".to_string(),
        operation,
        permission_type: PermissionType::Allow,
    }
}

fn deny(resource_type: ResourceType, operation: AclOperation) -> AclEntry {
    AclEntry {
        permission_type: PermissionType::Deny,
        ..allow(resource_type, operation)
    }
}

#[test]
fn derives_topic_admin_capabilities_from_topic_acls() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::Describe),
        allow(ResourceType::Topic, AclOperation::Create),
        allow(ResourceType::Topic, AclOperation::AlterConfigs),
        allow(ResourceType::Topic, AclOperation::Delete),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(capabilities.can_view_topics);
    assert!(capabilities.can_create_topics);
    assert!(capabilities.can_alter_topics);
    assert!(capabilities.can_delete_topics);
    assert_eq!(
        capabilities,
        Capabilities {
            can_view_topics: true,
            can_create_topics: true,
            can_alter_topics: true,
            can_delete_topics: true,
            ..Capabilities::default()
        }
    );
}

#[test]
fn unrelated_principal_gets_no_capabilities() {
    let mut entry = allow(ResourceType::Cluster, AclOperation::All);
    entry.principal = "User:bob".to_string();

    let capabilities = derive_capabilities("User:alice", &[entry]);

    assert_eq!(capabilities, Capabilities::default());
}

#[test]
fn deny_entries_are_ignored_for_first_version() {
    let entries = vec![deny(ResourceType::Topic, AclOperation::Describe)];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert_eq!(capabilities, Capabilities::default());
}

#[test]
fn all_grants_resource_specific_capabilities() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::All),
        allow(ResourceType::Group, AclOperation::All),
        allow(ResourceType::Cluster, AclOperation::All),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(capabilities.can_view_topics);
    assert!(capabilities.can_create_topics);
    assert!(capabilities.can_alter_topics);
    assert!(capabilities.can_delete_topics);
    assert!(capabilities.can_view_groups);
    assert!(capabilities.can_view_acls);
    assert!(capabilities.can_alter_acls);
    assert!(capabilities.can_alter_users);
    assert!(capabilities.can_view_quotas);
    assert!(capabilities.can_alter_quotas);
    assert!(capabilities.can_view_log_dirs);
}
