use std::collections::BTreeMap;

use crabka_admin_ui::{
    admin::{acl_rows, group_rows, log_dir_rows, quota_rows, resource_outcome_rows, topic_rows},
    dto::{
        AclRow, ConfigEntryDto, CreateTopicRequestDto, GroupRow, KafkaErrorDto,
        LogDirMoveRequestDto, LogDirRow, QuotaRow, ResourceOutcome, ScramUserUpsertDto, TopicRow,
        UserRow,
    },
    error::UiError,
};
use crabka_client_admin::{
    AclEntry, AclOperation, AdminError, AlterReplicaLogDirOutcome, CreatePartitionsOutcome,
    DeleteAclFilterOutcome, DeleteTopicOutcome, KafkaError, LogDirInfo, LogDirPartitionInfo,
    LogDirTopicInfo, PatternType, PermissionType, ResourceType, ScramUserOutcome, TopicMetadata,
    TopicMetadataEntry, UserScramCredential, UserScramCredentials,
};

#[test]
fn resource_outcome_reports_error_state() {
    for (name, outcome, expected) in [
        ("successful resource", ResourceOutcome::ok("orders"), false),
        (
            "failed resource",
            ResourceOutcome::failed(
                "orders",
                KafkaErrorDto {
                    code: 36,
                    name: "TOPIC_ALREADY_EXISTS".to_string(),
                    message: Some("topic exists".to_string()),
                },
            ),
            true,
        ),
    ] {
        assert_eq!(outcome.has_error(), expected, "case {name}");
    }
}

#[test]
fn create_topic_request_accepts_valid_topic_shape() {
    let request = CreateTopicRequestDto {
        name: "orders".to_string(),
        partitions: 3,
        replicas: 1,
        configs: vec![ConfigEntryDto {
            name: "cleanup.policy".to_string(),
            value: "delete".to_string(),
        }],
    };

    assert!(request.validate().is_ok());
}

#[test]
fn create_topic_request_rejects_invalid_counts_and_blank_names() {
    for (name, request) in [
        (
            "zero partitions",
            CreateTopicRequestDto {
                name: "orders".to_string(),
                partitions: 0,
                replicas: 1,
                configs: Vec::new(),
            },
        ),
        (
            "blank topic name",
            CreateTopicRequestDto {
                name: " ".to_string(),
                partitions: 1,
                replicas: 1,
                configs: Vec::new(),
            },
        ),
        (
            "blank config name",
            CreateTopicRequestDto {
                name: "orders".to_string(),
                partitions: 1,
                replicas: 1,
                configs: vec![ConfigEntryDto {
                    name: "\t".to_string(),
                    value: "delete".to_string(),
                }],
            },
        ),
    ] {
        assert!(request.validate().is_err(), "case {name}");
    }
}

#[test]
fn scram_upsert_request_rejects_empty_password_and_bad_iterations() {
    for (name, request) in [
        (
            "empty password",
            ScramUserUpsertDto {
                username: "alice".to_string(),
                password: String::new(),
                iterations: 4096,
            },
        ),
        (
            "zero iterations",
            ScramUserUpsertDto {
                username: "alice".to_string(),
                password: "not-asserted".to_string(),
                iterations: 0,
            },
        ),
    ] {
        assert!(request.validate().is_err(), "case {name}");
    }
}

#[test]
fn scram_upsert_debug_redacts_password() {
    let password_sentinel = "scram-debug-password-sentinel";
    let request = ScramUserUpsertDto {
        username: "alice".to_string(),
        password: password_sentinel.to_string(),
        iterations: 4096,
    };

    let debug = format!("{request:?}");

    assert!(
        !debug.contains(password_sentinel),
        "debug output leaked password"
    );
    assert!(debug.contains("<redacted>"));
}

#[test]
fn log_dir_move_request_rejects_nonsensical_fields() {
    let request = LogDirMoveRequestDto {
        topic: "orders".to_string(),
        partition: -1,
        destination_log_dir: " ".to_string(),
    };

    assert!(request.validate().is_err());
}

#[test]
fn kafka_error_dto_from_kafka_error_preserves_fields() {
    let error = KafkaError {
        code: 36,
        name: "TOPIC_ALREADY_EXISTS",
        message: Some("topic exists".to_string()),
    };

    let dto = KafkaErrorDto::from(&error);

    assert_eq!(
        dto,
        KafkaErrorDto {
            code: 36,
            name: "TOPIC_ALREADY_EXISTS".to_string(),
            message: Some("topic exists".to_string()),
        }
    );
}

#[test]
fn ui_error_from_broker_admin_error_preserves_structured_fields() {
    let error = AdminError::Broker {
        api: "CreateTopics",
        code: 36,
        name: "TOPIC_ALREADY_EXISTS",
        message: Some("topic exists".to_string()),
    };

    let ui_error = UiError::from(error);

    assert_eq!(
        ui_error,
        UiError::Broker {
            api: "CreateTopics",
            code: 36,
            name: "TOPIC_ALREADY_EXISTS".to_string(),
            message: Some("topic exists".to_string()),
        }
    );
}

#[test]
fn maps_topic_metadata_to_rows_with_errors() {
    let topic_id =
        uuid::Uuid::parse_str("018f5a30-14a1-7b29-9f4d-4cbb6425492a").expect("valid topic id");
    let metadata = TopicMetadata {
        controller_id: 2,
        topics: vec![
            TopicMetadataEntry {
                name: "orders".to_string(),
                topic_id: Some(topic_id),
                partition_count: 3,
                replication_factor: 2,
                error: None,
            },
            TopicMetadataEntry {
                name: "missing".to_string(),
                topic_id: None,
                partition_count: 0,
                replication_factor: 0,
                error: Some(KafkaError {
                    code: 3,
                    name: "UNKNOWN_TOPIC_OR_PARTITION",
                    message: Some("topic missing".to_string()),
                }),
            },
        ],
    };

    let rows = topic_rows(metadata);

    assert_eq!(
        rows,
        [
            TopicRow {
                name: "orders".to_string(),
                topic_id: Some(topic_id.to_string()),
                partition_count: 3,
                replication_factor: 2,
                error: None,
            },
            TopicRow {
                name: "missing".to_string(),
                topic_id: None,
                partition_count: 0,
                replication_factor: 0,
                error: Some(KafkaErrorDto {
                    code: 3,
                    name: "UNKNOWN_TOPIC_OR_PARTITION".to_string(),
                    message: Some("topic missing".to_string()),
                }),
            },
        ]
    );
}

#[test]
fn maps_group_ids_to_group_rows() {
    let rows = group_rows(vec!["payments".to_string(), "shipping".to_string()]);

    assert_eq!(
        rows,
        [
            GroupRow {
                group_id: "payments".to_string(),
            },
            GroupRow {
                group_id: "shipping".to_string(),
            },
        ]
    );
}

#[test]
fn maps_acl_entries_to_visible_rows() {
    let rows = acl_rows(vec![AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "orders".to_string(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".to_string(),
        host: "*".to_string(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }]);

    assert_eq!(
        rows,
        [AclRow {
            resource: "Topic:orders (Literal)".to_string(),
            principal: "User:alice".to_string(),
            operation: "Read".to_string(),
            permission: "Allow".to_string(),
        }]
    );
}

#[test]
fn maps_user_quota_config_to_visible_rows() {
    let quotas = BTreeMap::from([
        ("consumer_byte_rate".to_string(), 2048.0),
        ("producer_byte_rate".to_string(), 1024.5),
    ]);

    let rows = quota_rows("alice", quotas);

    assert_eq!(
        rows,
        [
            QuotaRow {
                entity: "alice".to_string(),
                quota_type: "consumer_byte_rate".to_string(),
                value: "2048".to_string(),
            },
            QuotaRow {
                entity: "alice".to_string(),
                quota_type: "producer_byte_rate".to_string(),
                value: "1024.5".to_string(),
            },
        ]
    );
}

#[test]
fn maps_scram_credentials_to_user_rows() {
    let rows = crabka_admin_ui::admin::user_rows(vec![UserScramCredentials {
        username: "alice".to_string(),
        credentials: vec![UserScramCredential {
            mechanism: "SCRAM-SHA-512".to_string(),
            iterations: 8192,
        }],
        error: None,
    }]);

    assert_eq!(
        rows,
        [UserRow {
            username: "alice".to_string(),
            principal: "SCRAM-SHA-512".to_string(),
        }]
    );
}

#[test]
fn maps_mutation_outcomes_preserving_kafka_errors() {
    let error = KafkaError {
        code: 3,
        name: "UNKNOWN_TOPIC_OR_PARTITION",
        message: Some("missing".to_string()),
    };

    let delete_topic_rows = resource_outcome_rows(vec![DeleteTopicOutcome {
        name: "orders".to_string(),
        error: Some(error.clone()),
    }]);
    let partition_rows = resource_outcome_rows(vec![CreatePartitionsOutcome {
        name: "payments".to_string(),
        error: None,
    }]);
    let scram_rows = resource_outcome_rows(vec![ScramUserOutcome {
        username: "alice".to_string(),
        error: Some(error.clone()),
    }]);
    let log_dir_rows = resource_outcome_rows(vec![AlterReplicaLogDirOutcome {
        topic: "orders".to_string(),
        partition: 1,
        error: None,
    }]);
    let delete_acl_rows = resource_outcome_rows(vec![DeleteAclFilterOutcome {
        error: Some(error),
        matched: Vec::new(),
    }]);

    let expected_error = Some(KafkaErrorDto {
        code: 3,
        name: "UNKNOWN_TOPIC_OR_PARTITION".to_string(),
        message: Some("missing".to_string()),
    });
    assert_eq!(
        (
            delete_topic_rows,
            partition_rows,
            scram_rows,
            log_dir_rows,
            delete_acl_rows,
        ),
        (
            vec![ResourceOutcome {
                resource: "orders".to_string(),
                error: expected_error.clone(),
            }],
            vec![ResourceOutcome::ok("payments")],
            vec![ResourceOutcome {
                resource: "alice".to_string(),
                error: expected_error.clone(),
            }],
            vec![ResourceOutcome::ok("orders-1")],
            vec![ResourceOutcome {
                resource: "acl-filter".to_string(),
                error: expected_error,
            }],
        )
    );
}

#[test]
fn maps_log_dir_info_to_partition_rows_with_directory_errors() {
    let log_dirs = vec![LogDirInfo {
        log_dir: "/var/lib/crabka-0".to_string(),
        error: Some(KafkaError {
            code: 57,
            name: "KAFKA_STORAGE_ERROR",
            message: Some("disk offline".to_string()),
        }),
        topics: vec![LogDirTopicInfo {
            name: "orders".to_string(),
            partitions: vec![
                LogDirPartitionInfo {
                    partition_index: 0,
                    partition_size: 1024,
                    offset_lag: 0,
                    is_future_key: false,
                },
                LogDirPartitionInfo {
                    partition_index: 1,
                    partition_size: 2048,
                    offset_lag: 7,
                    is_future_key: true,
                },
            ],
        }],
    }];

    let rows = log_dir_rows(log_dirs);

    let error = Some(KafkaErrorDto {
        code: 57,
        name: "KAFKA_STORAGE_ERROR".to_string(),
        message: Some("disk offline".to_string()),
    });
    assert_eq!(
        rows,
        [
            LogDirRow {
                log_dir: "/var/lib/crabka-0".to_string(),
                topic: "orders".to_string(),
                partition: 0,
                partition_size: 1024,
                offset_lag: 0,
                is_future_key: false,
                error: error.clone(),
            },
            LogDirRow {
                log_dir: "/var/lib/crabka-0".to_string(),
                topic: "orders".to_string(),
                partition: 1,
                partition_size: 2048,
                offset_lag: 7,
                is_future_key: true,
                error,
            },
        ]
    );
}

#[test]
fn maps_errored_empty_log_dir_to_sentinel_row() {
    let log_dirs = vec![LogDirInfo {
        log_dir: "/var/lib/crabka-offline".to_string(),
        error: Some(KafkaError {
            code: 57,
            name: "KAFKA_STORAGE_ERROR",
            message: Some("disk offline".to_string()),
        }),
        topics: Vec::new(),
    }];

    let rows = log_dir_rows(log_dirs);

    assert_eq!(
        rows,
        [LogDirRow {
            log_dir: "/var/lib/crabka-offline".to_string(),
            topic: String::new(),
            partition: -1,
            partition_size: 0,
            offset_lag: 0,
            is_future_key: false,
            error: Some(KafkaErrorDto {
                code: 57,
                name: "KAFKA_STORAGE_ERROR".to_string(),
                message: Some("disk offline".to_string()),
            }),
        }]
    );
}
