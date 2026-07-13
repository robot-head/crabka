use crabka_admin_ui::permissions::{
    Capabilities, derive_capabilities, derive_capabilities_for_host,
};
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

fn wildcard_allow(resource_type: ResourceType, operation: AclOperation) -> AclEntry {
    AclEntry {
        principal: "User:*".to_string(),
        ..allow(resource_type, operation)
    }
}

fn wildcard_deny(resource_type: ResourceType, operation: AclOperation) -> AclEntry {
    AclEntry {
        principal: "User:*".to_string(),
        ..deny(resource_type, operation)
    }
}

fn host_allow(resource_type: ResourceType, operation: AclOperation, host: &str) -> AclEntry {
    AclEntry {
        host: host.to_string(),
        ..allow(resource_type, operation)
    }
}

fn host_deny(resource_type: ResourceType, operation: AclOperation, host: &str) -> AclEntry {
    AclEntry {
        host: host.to_string(),
        ..deny(resource_type, operation)
    }
}

#[test]
fn derives_topic_admin_capabilities_from_topic_acls() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::Describe),
        allow(ResourceType::Topic, AclOperation::AlterConfigs),
        allow(ResourceType::Topic, AclOperation::Delete),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(capabilities.can_view_topics());
    assert!(!capabilities.can_create_topics());
    assert!(capabilities.can_alter_topics());
    assert!(capabilities.can_delete_topics());
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

    assert!(capabilities.can_view_topics());
    assert!(capabilities.can_create_topics());
    assert!(capabilities.can_alter_topics());
    assert!(capabilities.can_delete_topics());
    assert!(capabilities.can_view_groups());
    assert!(capabilities.can_view_acls());
    assert!(capabilities.can_alter_acls());
    assert!(capabilities.can_alter_users());
    assert!(capabilities.can_view_quotas());
    assert!(capabilities.can_alter_quotas());
    assert!(capabilities.can_view_log_dirs());
}

#[test]
fn cluster_describe_configs_does_not_grant_acl_view() {
    let entries = vec![allow(ResourceType::Cluster, AclOperation::DescribeConfigs)];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(!capabilities.can_view_acls());
    assert!(!capabilities.can_view_quotas());
    assert_eq!(capabilities, Capabilities::default());
}

#[test]
fn cluster_alter_configs_does_not_grant_acl_or_user_admin() {
    let entries = vec![allow(ResourceType::Cluster, AclOperation::AlterConfigs)];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(!capabilities.can_alter_acls());
    assert!(!capabilities.can_alter_users());
    assert!(!capabilities.can_view_quotas());
    assert!(!capabilities.can_alter_quotas());
    assert_eq!(capabilities, Capabilities::default());
}

#[test]
fn wildcard_principal_grants_matching_user_capabilities() {
    let entries = vec![wildcard_allow(ResourceType::Topic, AclOperation::Describe)];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(capabilities.can_view_topics());
}

#[test]
fn topic_mutating_operations_imply_topic_view() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::Write),
        allow(ResourceType::Topic, AclOperation::Alter),
        allow(ResourceType::Topic, AclOperation::Delete),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(capabilities.can_view_topics());
    assert!(capabilities.can_alter_topics());
    assert!(capabilities.can_delete_topics());
}

#[test]
fn topic_alter_configs_does_not_imply_topic_view() {
    let entries = vec![allow(ResourceType::Topic, AclOperation::AlterConfigs)];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(!capabilities.can_view_topics());
    assert!(capabilities.can_alter_topics());
}

#[test]
fn denying_topic_alter_configs_does_not_mask_topic_alter() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::Alter),
        deny(ResourceType::Topic, AclOperation::AlterConfigs),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(capabilities.can_view_topics());
    assert!(capabilities.can_alter_topics());
}

#[test]
fn denying_topic_alter_does_not_mask_topic_alter_configs() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::AlterConfigs),
        deny(ResourceType::Topic, AclOperation::Alter),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(!capabilities.can_view_topics());
    assert!(capabilities.can_alter_topics());
}

#[test]
fn denying_topic_all_masks_topic_capabilities_broadly() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::All),
        deny(ResourceType::Topic, AclOperation::All),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(!capabilities.can_view_topics());
    assert!(!capabilities.can_alter_topics());
    assert!(!capabilities.can_delete_topics());
}

#[test]
fn cluster_create_grants_topic_creation() {
    let entries = vec![allow(ResourceType::Cluster, AclOperation::Create)];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(capabilities.can_create_topics());
}

#[test]
fn cluster_describe_and_alter_are_quota_grants_not_config_ops() {
    let describe = derive_capabilities(
        "User:alice",
        &[allow(ResourceType::Cluster, AclOperation::Describe)],
    );
    let alter = derive_capabilities(
        "User:alice",
        &[allow(ResourceType::Cluster, AclOperation::Alter)],
    );

    assert!(describe.can_view_quotas());
    assert!(!describe.can_alter_quotas());
    assert!(alter.can_view_quotas());
    assert!(alter.can_alter_quotas());
}

#[test]
fn exact_deny_takes_precedence_over_allow_for_topic_capabilities() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::All),
        deny(ResourceType::Topic, AclOperation::Describe),
        allow(ResourceType::Cluster, AclOperation::Create),
        deny(ResourceType::Cluster, AclOperation::Create),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(!capabilities.can_view_topics());
    assert!(!capabilities.can_create_topics());
    assert!(capabilities.can_alter_topics());
    assert!(capabilities.can_delete_topics());
}

#[test]
fn wildcard_deny_takes_precedence_for_cluster_admin_and_quotas() {
    let entries = vec![
        allow(ResourceType::Cluster, AclOperation::All),
        wildcard_deny(ResourceType::Cluster, AclOperation::Alter),
    ];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(!capabilities.can_view_acls());
    assert!(!capabilities.can_alter_acls());
    assert!(!capabilities.can_alter_users());
    assert!(!capabilities.can_view_quotas());
    assert!(!capabilities.can_alter_quotas());
}

#[test]
fn cluster_alter_implies_acl_view_and_admin_capabilities() {
    let entries = vec![allow(ResourceType::Cluster, AclOperation::Alter)];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert!(capabilities.can_view_acls());
    assert!(capabilities.can_alter_acls());
    assert!(capabilities.can_alter_users());
}

#[test]
fn derive_capabilities_ignores_host_specific_allow_without_peer_host() {
    let entries = vec![host_allow(
        ResourceType::Topic,
        AclOperation::Describe,
        "10.0.0.1",
    )];

    let capabilities = derive_capabilities("User:alice", &entries);

    assert_eq!(capabilities, Capabilities::default());
}

#[test]
fn derive_capabilities_for_host_matches_exact_or_wildcard_host() {
    let entries = vec![
        host_allow(ResourceType::Topic, AclOperation::Describe, "10.0.0.1"),
        allow(ResourceType::Cluster, AclOperation::Create),
    ];

    let matching = derive_capabilities_for_host("User:alice", "10.0.0.1", &entries);
    let nonmatching = derive_capabilities_for_host("User:alice", "10.0.0.2", &entries);

    assert!(matching.can_view_topics());
    assert!(matching.can_create_topics());
    assert!(!nonmatching.can_view_topics());
    assert!(nonmatching.can_create_topics());
}

#[test]
fn host_matching_controls_deny_precedence() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::Describe),
        host_deny(ResourceType::Topic, AclOperation::Describe, "10.0.0.2"),
    ];

    let matching_deny = derive_capabilities_for_host("User:alice", "10.0.0.2", &entries);
    let nonmatching_deny = derive_capabilities_for_host("User:alice", "10.0.0.1", &entries);

    assert!(!matching_deny.can_view_topics());
    assert!(nonmatching_deny.can_view_topics());
}
