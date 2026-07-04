use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigEntryDto {
    pub name: String,
    pub value: String,
}

impl ConfigEntryDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("config name", &self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTopicRequestDto {
    pub name: String,
    pub partitions: i32,
    pub replicas: i32,
    pub configs: Vec<ConfigEntryDto>,
}

impl CreateTopicRequestDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("topic name", &self.name)?;
        ensure_positive("partition count", self.partitions)?;
        ensure_positive("replica count", self.replicas)?;

        for config in &self.configs {
            config.validate()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicRequestDto {
    pub name: String,
}

impl DeleteTopicRequestDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("topic name", &self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatePartitionsRequestDto {
    pub topic: String,
    pub total_count: i32,
}

impl CreatePartitionsRequestDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("topic name", &self.topic)?;
        ensure_positive("partition count", self.total_count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterConfigRequestDto {
    pub resource_type: String,
    pub resource_name: String,
    pub configs: Vec<ConfigEntryDto>,
}

impl AlterConfigRequestDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("config resource type", &self.resource_type)?;
        ensure_not_blank("config resource name", &self.resource_name)?;

        for config in &self.configs {
            config.validate()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclRequestDto {
    pub resource_type: String,
    pub resource_name: String,
    pub principal: String,
    pub operation: String,
    pub permission: String,
    pub host: String,
}

impl AclRequestDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("ACL resource type", &self.resource_type)?;
        ensure_not_blank("ACL resource name", &self.resource_name)?;
        ensure_not_blank("ACL principal", &self.principal)?;
        ensure_not_blank("ACL operation", &self.operation)?;
        ensure_not_blank("ACL permission", &self.permission)?;
        ensure_not_blank("ACL host", &self.host)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramUserUpsertDto {
    pub username: String,
    pub password: String,
    pub iterations: i32,
}

impl ScramUserUpsertDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("SCRAM username", &self.username)?;
        ensure_not_blank("SCRAM password", &self.password)?;
        ensure_positive("SCRAM iterations", self.iterations)
    }
}

impl fmt::Debug for ScramUserUpsertDto {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramUserUpsertDto")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("iterations", &self.iterations)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramUserDeleteDto {
    pub username: String,
}

impl ScramUserDeleteDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("SCRAM username", &self.username)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaUpsertDto {
    pub entity: String,
    pub quota_type: String,
    pub value: f64,
}

impl QuotaUpsertDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("quota entity", &self.entity)?;
        ensure_not_blank("quota type", &self.quota_type)?;
        ensure_finite("quota value", self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaDeleteDto {
    pub entity: String,
    pub quota_type: String,
}

impl QuotaDeleteDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("quota entity", &self.entity)?;
        ensure_not_blank("quota type", &self.quota_type)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogDirMoveRequestDto {
    pub topic: String,
    pub partition: i32,
    pub destination_log_dir: String,
}

impl LogDirMoveRequestDto {
    pub fn validate(&self) -> Result<(), String> {
        ensure_not_blank("log-dir move topic", &self.topic)?;
        ensure_nonnegative("log-dir move partition", self.partition)?;
        ensure_not_blank("destination log dir", &self.destination_log_dir)
    }
}

fn ensure_not_blank(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be blank"));
    }

    Ok(())
}

fn ensure_positive(field: &str, value: i32) -> Result<(), String> {
    if value <= 0 {
        return Err(format!("{field} must be positive"));
    }

    Ok(())
}

fn ensure_nonnegative(field: &str, value: i32) -> Result<(), String> {
    if value < 0 {
        return Err(format!("{field} must be nonnegative"));
    }

    Ok(())
}

fn ensure_finite(field: &str, value: f64) -> Result<(), String> {
    if !value.is_finite() {
        return Err(format!("{field} must be finite"));
    }

    Ok(())
}

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
