use crabka_connect::ConnectError;

#[derive(Debug, thiserror::Error)]
pub enum PostgresConnectError {
    #[error("postgres backend error: {0}")]
    Backend(String),
    #[error("postgres offset error: {0}")]
    Offset(String),
    #[error("postgres conversion error: {0}")]
    Convert(String),
}

impl From<PostgresConnectError> for ConnectError {
    fn from(value: PostgresConnectError) -> Self {
        match value {
            PostgresConnectError::Backend(message) => Self::Backend(message),
            PostgresConnectError::Offset(message) => Self::Offset(message),
            PostgresConnectError::Convert(message) => Self::Convert(message),
        }
    }
}
