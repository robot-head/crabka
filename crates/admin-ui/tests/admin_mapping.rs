use crabka_admin_ui::dto::{KafkaErrorDto, ResourceOutcome};
use crabka_admin_ui::error::UiError;
use crabka_client_admin::{AdminError, KafkaError};

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
