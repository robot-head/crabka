use crate::SaslMechanism;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub mechanism: SaslMechanism,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    #[error("unknown user")]
    UnknownUser,
    #[error("bad password")]
    BadPassword,
    #[error("bad proof")]
    BadProof,
    #[error("malformed message")]
    MalformedMessage,
    #[error("unsupported mechanism")]
    UnsupportedMechanism,
}
