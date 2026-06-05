//! Gateway error type. Wraps native-client errors so handlers can map to
//! Connect status without leaking client internals.

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
    #[error("dedup store is not yet warmed up")]
    NotReady,
    #[error("dedup claim could not be (de)serialized: {0}")]
    Claim(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}
