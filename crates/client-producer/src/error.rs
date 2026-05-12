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

    #[error("compression: {0}")]
    Compression(#[from] crabka_compression::CompressionError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_fenced_producer() {
        assert!(ProducerError::FencedProducer.to_string().contains("fenced"));
    }

    #[test]
    fn display_invalid_config() {
        let e = ProducerError::InvalidConfig("idempotence requires acks=all");
        assert!(e.to_string().contains("idempotence"));
    }
}
