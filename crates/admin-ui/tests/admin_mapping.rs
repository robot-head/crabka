use crabka_admin_ui::admin::{
    acl_rows, group_rows, log_dir_rows, quota_rows, resource_outcome_rows, topic_rows,
};
use crabka_admin_ui::dto::{
    ConfigEntryDto, CreateTopicRequestDto, KafkaErrorDto, LogDirMoveRequestDto, ResourceOutcome,
    ScramUserUpsertDto,
};
use crabka_admin_ui::error::UiError;
use crabka_client_admin::{
    AclEntry, AclOperation, AdminError, AlterReplicaLogDirOutcome, CreatePartitionsOutcome,
    DeleteAclFilterOutcome, DeleteTopicOutcome, KafkaError, LogDirInfo, LogDirPartitionInfo,
    LogDirTopicInfo, PatternType, PermissionType, ResourceType, ScramUserOutcome, TopicMetadata,
    TopicMetadataEntry, UserScramCredential, UserScramCredentials,
};
use std::collections::BTreeMap;

#[test]
fn resource_outcome_reports_error_state() {
    let ok = ResourceOutcome::ok("orders");
    let failed = ResourceOutcome::failed(
        "orders",
        KafkaErrorDto {
            code: 36,
            name: "TOPIC_ALREADY_EXISTS".to_string(),
            message: Some("topic exists".to_string()),
        },
    );

    assert!(!ok.has_error());
    assert!(failed.has_error());
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
    let zero_partitions = CreateTopicRequestDto {
        name: "orders".to_string(),
        partitions: 0,
        replicas: 1,
        configs: Vec::new(),
    };
    let blank_name = CreateTopicRequestDto {
        name: " ".to_string(),
        partitions: 1,
        replicas: 1,
        configs: Vec::new(),
    };
    let blank_config = CreateTopicRequestDto {
        name: "orders".to_string(),
        partitions: 1,
        replicas: 1,
        configs: vec![ConfigEntryDto {
            name: "\t".to_string(),
            value: "delete".to_string(),
        }],
    };

    assert!(zero_partitions.validate().is_err());
    assert!(blank_name.validate().is_err());
    assert!(blank_config.validate().is_err());
}

#[test]
fn scram_upsert_request_rejects_empty_password_and_bad_iterations() {
    let empty_password = ScramUserUpsertDto {
        username: "alice".to_string(),
        password: String::new(),
        iterations: 4096,
    };
    let zero_iterations = ScramUserUpsertDto {
        username: "alice".to_string(),
        password: "not-asserted".to_string(),
        iterations: 0,
    };

    assert!(empty_password.validate().is_err());
    assert!(zero_iterations.validate().is_err());
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

    assert_eq!(dto.code, 36);
    assert_eq!(dto.name, "TOPIC_ALREADY_EXISTS");
    assert_eq!(dto.message, Some("topic exists".to_string()));
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

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "orders");
    assert_eq!(rows[0].topic_id, Some(topic_id.to_string()));
    assert_eq!(rows[0].partition_count, 3);
    assert_eq!(rows[0].replication_factor, 2);
    assert_eq!(rows[0].error, None);
    assert_eq!(rows[1].name, "missing");
    assert_eq!(
        rows[1].error,
        Some(KafkaErrorDto {
            code: 3,
            name: "UNKNOWN_TOPIC_OR_PARTITION".to_string(),
            message: Some("topic missing".to_string()),
        })
    );
}

#[test]
fn maps_group_ids_to_group_rows() {
    let rows = group_rows(vec!["payments".to_string(), "shipping".to_string()]);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].group_id, "payments");
    assert_eq!(rows[1].group_id, "shipping");
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

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].resource, "Topic:orders (Literal)");
    assert_eq!(rows[0].principal, "User:alice");
    assert_eq!(rows[0].operation, "Read");
    assert_eq!(rows[0].permission, "Allow");
}

#[test]
fn maps_user_quota_config_to_visible_rows() {
    let quotas = BTreeMap::from([
        ("consumer_byte_rate".to_string(), 2048.0),
        ("producer_byte_rate".to_string(), 1024.5),
    ]);

    let rows = quota_rows("alice", quotas);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].entity, "alice");
    assert_eq!(rows[0].quota_type, "consumer_byte_rate");
    assert_eq!(rows[0].value, "2048");
    assert_eq!(rows[1].quota_type, "producer_byte_rate");
    assert_eq!(rows[1].value, "1024.5");
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

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].username, "alice");
    assert_eq!(rows[0].principal, "SCRAM-SHA-512");
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

    assert_eq!(delete_topic_rows[0].resource, "orders");
    assert!(delete_topic_rows[0].has_error());
    assert_eq!(partition_rows[0].resource, "payments");
    assert_eq!(scram_rows[0].resource, "alice");
    assert_eq!(log_dir_rows[0].resource, "orders-1");
    assert!(delete_acl_rows[0].has_error());
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

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].log_dir, "/var/lib/crabka-0");
    assert_eq!(rows[0].topic, "orders");
    assert_eq!(rows[0].partition, 0);
    assert_eq!(rows[0].partition_size, 1024);
    assert_eq!(rows[0].offset_lag, 0);
    assert!(!rows[0].is_future_key);
    assert_eq!(rows[1].partition, 1);
    assert_eq!(rows[1].partition_size, 2048);
    assert_eq!(rows[1].offset_lag, 7);
    assert!(rows[1].is_future_key);
    assert_eq!(
        rows[0].error,
        Some(KafkaErrorDto {
            code: 57,
            name: "KAFKA_STORAGE_ERROR".to_string(),
            message: Some("disk offline".to_string()),
        })
    );
    assert_eq!(rows[1].error, rows[0].error);
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

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].log_dir, "/var/lib/crabka-offline");
    assert_eq!(rows[0].topic, "");
    assert_eq!(rows[0].partition, -1);
    assert_eq!(rows[0].partition_size, 0);
    assert_eq!(rows[0].offset_lag, 0);
    assert!(!rows[0].is_future_key);
    assert_eq!(
        rows[0].error,
        Some(KafkaErrorDto {
            code: 57,
            name: "KAFKA_STORAGE_ERROR".to_string(),
            message: Some("disk offline".to_string()),
        })
    );
}
