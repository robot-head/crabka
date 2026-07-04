use crabka_client_admin::{AdminError, KafkaError};
use thiserror::Error;

use crate::dto::KafkaErrorDto;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UiError {
    #[error("not authenticated")]
    NotAuthenticated,
    #[error("session expired")]
    SessionExpired,
    #[error("broker connection failed: {0}")]
    BrokerConnection(String),
    #[error("broker returned error: api={api} code={code} ({name}){detail}",
        detail = .message.as_deref().map(|message| format!(" {message:?}")).unwrap_or_default())]
    Broker {
        api: &'static str,
        code: i16,
        name: String,
        message: Option<String>,
    },
    #[error("admin operation failed: {0}")]
    Admin(String),
}

impl From<&KafkaError> for KafkaErrorDto {
    fn from(error: &KafkaError) -> Self {
        Self {
            code: error.code,
            name: error.name.to_string(),
            message: error.message.clone(),
        }
    }
}

impl From<AdminError> for UiError {
    fn from(error: AdminError) -> Self {
        match error {
            AdminError::Connect { tried } => {
                Self::BrokerConnection(format!("no bootstrap address was reachable: tried {tried}"))
            }
            AdminError::Broker {
                api,
                code,
                name,
                message,
            } => Self::Broker {
                api,
                code,
                name: name.to_string(),
                message,
            },
            other => Self::Admin(other.to_string()),
        }
    }
}
