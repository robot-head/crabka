//! Gateway error type.
//!
//! [`GatewayError`] wraps native-client errors so handlers can map to Connect
//! status without leaking client internals.

use crabka_client_consumer::ConsumerError;
use crabka_client_producer::ProducerError;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("producer error: {0}")]
    Producer(#[from] ProducerError),
    #[error("producer send was canceled before acknowledgement")]
    ProducerCanceled,
    #[error("consumer error: {0}")]
    Consumer(#[from] ConsumerError),
    #[error("dedup partition not owned by this replica or still warming up")]
    Unavailable,
    #[error("forward to owner failed: {0}")]
    Forward(String),
    #[error("dedup claim could not be (de)serialized: {0}")]
    Claim(#[from] serde_json::Error),
    #[error("codec error: {0}")]
    Codec(#[from] crate::codec::CodecError),
    #[error("not authorized: {0}")]
    Unauthorized(String),
    #[error("{0}")]
    Other(String),
}
