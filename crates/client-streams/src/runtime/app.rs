//! stub — Task 5

/// Lifecycle state of a [`KafkaStreams`] runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaStreamsState {
    Created,
    Running,
    Closed,
}

/// Managed Kafka Streams runtime handle (filled in Task 5).
pub struct KafkaStreams;
