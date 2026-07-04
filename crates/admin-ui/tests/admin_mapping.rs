use crabka_admin_ui::admin::{log_dir_rows, topic_rows};
use crabka_admin_ui::dto::{KafkaErrorDto, ResourceOutcome};
use crabka_admin_ui::error::UiError;
use crabka_client_admin::{
    AdminError, KafkaError, LogDirInfo, LogDirPartitionInfo, LogDirTopicInfo, TopicMetadata,
    TopicMetadataEntry,
};

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
