use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataError {
    #[error("topic '{0}' already exists")]
    TopicExists(String),

    #[error("unknown topic '{0}'")]
    UnknownTopic(String),

    #[error("invalid partition {partition} on topic '{topic}'")]
    InvalidPartition { topic: String, partition: i32 },

    #[error("invalid record: {0}")]
    InvalidRecord(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_topic_exists() {
        let e = MetadataError::TopicExists("my-topic".into());
        assert_eq!(e.to_string(), "topic 'my-topic' already exists");
    }

    #[test]
    fn display_invalid_partition() {
        let e = MetadataError::InvalidPartition {
            topic: "t".into(),
            partition: 7,
        };
        assert!(e.to_string().contains("partition 7"));
    }
}
