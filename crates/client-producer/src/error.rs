use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProducerError {
    #[error("client: {0}")]
    Client(#[from] crabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    #[error("broker error_code {0}")]
    Server(i16),

    #[error("fenced by newer producer instance")]
    FencedProducer,

    #[error("invalid config: {0}")]
    InvalidConfig(&'static str),

    #[error("batch too large: {batch_size} > max")]
    BatchTooLarge { batch_size: usize },

    #[error("record too large: {record_size} > max_request_size")]
    RecordTooLarge { record_size: usize },

    #[error("send buffer full (max_block exceeded)")]
    BufferFull,

    #[error("producer closed")]
    Closed,

    #[error("flush timed out")]
    FlushTimeout,

    #[error("compression: {0}")]
    Compression(#[from] crabka_compression::CompressionError),

    #[error("producer is not transactional (no transactional_id configured)")]
    NotTransactional,

    #[error("invalid transaction state: {0}")]
    InvalidTransactionState(&'static str),

    #[error("transaction was aborted by the broker (timeout or fence)")]
    TransactionAborted,

    #[error("concurrent transactions on the same transactional_id")]
    ConcurrentTransactions,

    #[error(
        "transaction outcome is unknown; call init_transactions before sending or beginning another transaction"
    )]
    RecoveryRequired,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn display_messages() {
        for (_name, error, expected) in [
            (
                "fenced producer",
                ProducerError::FencedProducer,
                "fenced by newer producer instance",
            ),
            (
                "invalid config",
                ProducerError::InvalidConfig("idempotence requires acks=all"),
                "invalid config: idempotence requires acks=all",
            ),
        ] {
            assert2::assert!(error.to_string() == expected);
        }
    }
}
