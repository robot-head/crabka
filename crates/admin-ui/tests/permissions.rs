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

fn assert_capability_cases(
    cases: impl IntoIterator<Item = (&'static str, Vec<AclEntry>, Capabilities)>,
) {
    for (_name, entries, expected) in cases {
        assert2::assert!(derive_capabilities("User:alice", &entries) == expected);
    }
}

#[test]
fn capability_grant_cases() {
    let mut other_principal = allow(ResourceType::Cluster, AclOperation::All);
    other_principal.principal = "User:bob".to_string();

    assert_capability_cases([
        (
            "topic administration grants",
            vec![
                allow(ResourceType::Topic, AclOperation::Describe),
                allow(ResourceType::Topic, AclOperation::AlterConfigs),
                allow(ResourceType::Topic, AclOperation::Delete),
            ],
            Capabilities {
                can_view_topics: true,
                can_alter_topics: true,
                can_delete_topics: true,
                ..Capabilities::default()
            },
        ),
        (
            "unrelated principal",
            vec![other_principal],
            Capabilities::default(),
        ),
        (
            "deny ignored without a matching allow",
            vec![deny(ResourceType::Topic, AclOperation::Describe)],
            Capabilities::default(),
        ),
        (
            "all grants by resource",
            vec![
                allow(ResourceType::Topic, AclOperation::All),
                allow(ResourceType::Group, AclOperation::All),
                allow(ResourceType::Cluster, AclOperation::All),
            ],
            Capabilities {
                can_view_topics: true,
                can_create_topics: true,
                can_alter_topics: true,
                can_delete_topics: true,
                can_view_groups: true,
                can_view_acls: true,
                can_alter_acls: true,
                can_alter_users: true,
                can_view_quotas: true,
                can_alter_quotas: true,
                can_view_log_dirs: true,
            },
        ),
        (
            "cluster describe-configs does not grant ACL view",
            vec![allow(ResourceType::Cluster, AclOperation::DescribeConfigs)],
            Capabilities::default(),
        ),
        (
            "cluster alter-configs does not grant administration",
            vec![allow(ResourceType::Cluster, AclOperation::AlterConfigs)],
            Capabilities::default(),
        ),
        (
            "wildcard principal",
            vec![wildcard_allow(ResourceType::Topic, AclOperation::Describe)],
            Capabilities {
                can_view_topics: true,
                ..Capabilities::default()
            },
        ),
        (
            "topic mutations imply view",
            vec![
                allow(ResourceType::Topic, AclOperation::Write),
                allow(ResourceType::Topic, AclOperation::Alter),
                allow(ResourceType::Topic, AclOperation::Delete),
            ],
            Capabilities {
                can_view_topics: true,
                can_alter_topics: true,
                can_delete_topics: true,
                ..Capabilities::default()
            },
        ),
        (
            "topic alter-configs does not imply view",
            vec![allow(ResourceType::Topic, AclOperation::AlterConfigs)],
            Capabilities {
                can_alter_topics: true,
                ..Capabilities::default()
            },
        ),
    ]);
}

#[test]
fn capability_deny_precedence_cases() {
    assert_capability_cases([
        (
            "deny alter-configs does not mask alter",
            vec![
                allow(ResourceType::Topic, AclOperation::Alter),
                deny(ResourceType::Topic, AclOperation::AlterConfigs),
            ],
            Capabilities {
                can_view_topics: true,
                can_alter_topics: true,
                ..Capabilities::default()
            },
        ),
        (
            "deny alter does not mask alter-configs",
            vec![
                allow(ResourceType::Topic, AclOperation::AlterConfigs),
                deny(ResourceType::Topic, AclOperation::Alter),
            ],
            Capabilities {
                can_alter_topics: true,
                ..Capabilities::default()
            },
        ),
        (
            "deny topic all masks topic capabilities",
            vec![
                allow(ResourceType::Topic, AclOperation::All),
                deny(ResourceType::Topic, AclOperation::All),
            ],
            Capabilities::default(),
        ),
        (
            "cluster create grants topic creation",
            vec![allow(ResourceType::Cluster, AclOperation::Create)],
            Capabilities {
                can_create_topics: true,
                ..Capabilities::default()
            },
        ),
        (
            "cluster describe grants read capabilities",
            vec![allow(ResourceType::Cluster, AclOperation::Describe)],
            Capabilities {
                can_view_acls: true,
                can_view_quotas: true,
                can_view_log_dirs: true,
                ..Capabilities::default()
            },
        ),
        (
            "cluster alter grants administration capabilities",
            vec![allow(ResourceType::Cluster, AclOperation::Alter)],
            Capabilities {
                can_view_acls: true,
                can_alter_acls: true,
                can_alter_users: true,
                can_view_quotas: true,
                can_alter_quotas: true,
                can_view_log_dirs: true,
                ..Capabilities::default()
            },
        ),
    ]);
}

#[test]
fn capability_host_cases() {
    assert_capability_cases([
        (
            "exact deny precedence",
            vec![
                allow(ResourceType::Topic, AclOperation::All),
                deny(ResourceType::Topic, AclOperation::Describe),
                allow(ResourceType::Cluster, AclOperation::Create),
                deny(ResourceType::Cluster, AclOperation::Create),
            ],
            Capabilities {
                can_alter_topics: true,
                can_delete_topics: true,
                ..Capabilities::default()
            },
        ),
        (
            "wildcard deny precedence",
            vec![
                allow(ResourceType::Cluster, AclOperation::All),
                wildcard_deny(ResourceType::Cluster, AclOperation::Alter),
            ],
            Capabilities {
                can_create_topics: true,
                ..Capabilities::default()
            },
        ),
        (
            "host-specific allow needs peer host",
            vec![host_allow(
                ResourceType::Topic,
                AclOperation::Describe,
                "10.0.0.1",
            )],
            Capabilities::default(),
        ),
    ]);
}

#[test]
fn derive_capabilities_for_host_matches_exact_or_wildcard_host() {
    let entries = vec![
        host_allow(ResourceType::Topic, AclOperation::Describe, "10.0.0.1"),
        allow(ResourceType::Cluster, AclOperation::Create),
    ];

    let matching = derive_capabilities_for_host("User:alice", "10.0.0.1", &entries);
    let nonmatching = derive_capabilities_for_host("User:alice", "10.0.0.2", &entries);

    assert2::assert!(
        matching
            == Capabilities {
                can_view_topics: true,
                can_create_topics: true,
                ..Capabilities::default()
            }
    );
    assert2::assert!(
        nonmatching
            == Capabilities {
                can_create_topics: true,
                ..Capabilities::default()
            }
    );
}

#[test]
fn host_matching_controls_deny_precedence() {
    let entries = vec![
        allow(ResourceType::Topic, AclOperation::Describe),
        host_deny(ResourceType::Topic, AclOperation::Describe, "10.0.0.2"),
    ];

    let matching_deny = derive_capabilities_for_host("User:alice", "10.0.0.2", &entries);
    let nonmatching_deny = derive_capabilities_for_host("User:alice", "10.0.0.1", &entries);

    assert2::assert!(matching_deny == Capabilities::default());
    assert2::assert!(
        nonmatching_deny
            == Capabilities {
                can_view_topics: true,
                ..Capabilities::default()
            }
    );
}
