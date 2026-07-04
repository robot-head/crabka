use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaErrorDto {
    pub code: i16,
    pub name: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceOutcome {
    pub resource: String,
    pub error: Option<KafkaErrorDto>,
}

impl ResourceOutcome {
    #[must_use]
    pub fn ok(resource: impl Into<String>) -> Self {
        Self {
            resource: resource.into(),
            error: None,
        }
    }

    #[must_use]
    pub fn failed(resource: impl Into<String>, error: KafkaErrorDto) -> Self {
        Self {
            resource: resource.into(),
            error: Some(error),
        }
    }

    #[must_use]
    pub const fn has_error(&self) -> bool {
        self.error.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRow {
    pub name: String,
    pub topic_id: Option<String>,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub error: Option<KafkaErrorDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRow {
    pub group_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogDirRow {
    pub log_dir: String,
    pub topic: String,
    pub partition: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future_key: bool,
    pub error: Option<KafkaErrorDto>,
}
