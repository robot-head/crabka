use std::collections::BTreeMap;

use crabka_client_admin::{
    AclEntry, AclEntryFilter, AdminClient, AdminError, AlterConfigsOutcome,
    AlterReplicaLogDirOutcome, CreateAclOutcome, CreatePartitionsOutcome, CreateTopicOutcome,
    DeleteAclFilterOutcome, DeleteTopicOutcome, KafkaError, LogDirInfo, ScramUserOutcome,
    TopicMetadata, UserQuotaConfig, UserScramCredential, UserScramCredentials,
};

use crate::dto::{
    AclRow, GroupRow, KafkaErrorDto, LogDirRow, QuotaRow, ResourceOutcome, TopicRow, UserRow,
};

pub struct AdminFacade {
    client: AdminClient,
}

impl AdminFacade {
    #[must_use]
    pub const fn new(client: AdminClient) -> Self {
        Self { client }
    }

    pub fn client_mut(&mut self) -> &mut AdminClient {
        &mut self.client
    }

    pub async fn topics(&mut self) -> Result<Vec<TopicRow>, AdminError> {
        let metadata = self.client.metadata(&[]).await?;

        Ok(topic_rows(metadata))
    }

    pub async fn groups(&mut self) -> Result<Vec<GroupRow>, AdminError> {
        let groups = self.client.list_groups().await?;

        Ok(group_rows(groups))
    }

    pub async fn log_dirs(&mut self) -> Result<Vec<LogDirRow>, AdminError> {
        let log_dirs = self.client.describe_log_dirs(None).await?;

        Ok(log_dir_rows(log_dirs))
    }

    pub async fn acls(&mut self) -> Result<Vec<AclRow>, AdminError> {
        let acls = self
            .client
            .describe_acls(&AclEntryFilter::default())
            .await?;

        Ok(acl_rows(acls))
    }

    pub async fn quotas_for_user(&mut self, username: &str) -> Result<Vec<QuotaRow>, AdminError> {
        let quotas = self.client.describe_user_quotas(username).await?;

        Ok(quota_rows(username, quotas))
    }

    pub async fn users(&mut self) -> Result<Vec<UserRow>, AdminError> {
        let users = self.client.describe_user_scram_credentials(None).await?;

        Ok(user_rows(users))
    }
}

#[must_use]
pub fn topic_rows(metadata: TopicMetadata) -> Vec<TopicRow> {
    metadata
        .topics
        .into_iter()
        .map(|topic| TopicRow {
            name: topic.name,
            topic_id: topic.topic_id.map(|id| id.to_string()),
            partition_count: topic.partition_count,
            replication_factor: topic.replication_factor,
            error: topic.error.as_ref().map(KafkaErrorDto::from),
        })
        .collect()
}

#[must_use]
pub fn group_rows(group_ids: Vec<String>) -> Vec<GroupRow> {
    group_ids
        .into_iter()
        .map(|group_id| GroupRow { group_id })
        .collect()
}

#[must_use]
pub fn acl_rows(acls: Vec<AclEntry>) -> Vec<AclRow> {
    acls.into_iter()
        .map(|acl| AclRow {
            resource: format!(
                "{:?}:{} ({:?})",
                acl.resource_type, acl.resource_name, acl.pattern_type
            ),
            principal: acl.principal,
            operation: format!("{:?}", acl.operation),
            permission: format!("{:?}", acl.permission_type),
        })
        .collect()
}

#[must_use]
pub fn quota_rows(username: &str, quotas: UserQuotaConfig) -> Vec<QuotaRow> {
    quotas
        .into_iter()
        .map(|(quota_type, value)| QuotaRow {
            entity: username.to_string(),
            quota_type,
            value: format_quota_value(value),
        })
        .collect()
}

#[must_use]
pub fn user_rows(users: Vec<UserScramCredentials>) -> Vec<UserRow> {
    users
        .into_iter()
        .map(|user| UserRow {
            username: user.username,
            principal: scram_credential_summary(&user.credentials),
        })
        .collect()
}

fn scram_credential_summary(credentials: &[UserScramCredential]) -> String {
    credentials
        .iter()
        .map(|credential| credential.mechanism.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

#[must_use]
pub fn resource_outcome_rows<T>(outcomes: Vec<T>) -> Vec<ResourceOutcome>
where
    T: IntoResourceOutcomeDto,
{
    outcomes
        .into_iter()
        .map(IntoResourceOutcomeDto::into_resource_outcome)
        .collect()
}

#[must_use]
pub fn quota_mutation_outcome(
    username: &str,
    quota_type: &str,
    error: Option<KafkaError>,
) -> ResourceOutcome {
    kafka_error_outcome(format!("{username}:{quota_type}"), error)
}

#[must_use]
pub fn log_dir_rows(log_dirs: Vec<LogDirInfo>) -> Vec<LogDirRow> {
    log_dirs
        .into_iter()
        .flat_map(log_dir_partition_rows)
        .collect()
}

fn log_dir_partition_rows(log_dir: LogDirInfo) -> Vec<LogDirRow> {
    let log_dir_name = log_dir.log_dir;
    let log_dir_error = log_dir.error.as_ref().map(KafkaErrorDto::from);

    if log_dir.topics.is_empty() && log_dir_error.is_some() {
        return vec![LogDirRow {
            log_dir: log_dir_name,
            topic: String::new(),
            partition: -1,
            partition_size: 0,
            offset_lag: 0,
            is_future_key: false,
            error: log_dir_error,
        }];
    }

    log_dir
        .topics
        .into_iter()
        .flat_map(move |topic| {
            let log_dir_name = log_dir_name.clone();
            let log_dir_error = log_dir_error.clone();

            topic
                .partitions
                .into_iter()
                .map(move |partition| LogDirRow {
                    log_dir: log_dir_name.clone(),
                    topic: topic.name.clone(),
                    partition: partition.partition_index,
                    partition_size: partition.partition_size,
                    offset_lag: partition.offset_lag,
                    is_future_key: partition.is_future_key,
                    error: log_dir_error.clone(),
                })
        })
        .collect()
}

fn format_quota_value(value: f64) -> String {
    if value.fract() == 0.0 {
        return format!("{value:.0}");
    }

    value.to_string()
}

fn kafka_error_outcome(resource: String, error: Option<KafkaError>) -> ResourceOutcome {
    ResourceOutcome {
        resource,
        error: error.map(|error| KafkaErrorDto {
            code: error.code,
            name: error.name.to_string(),
            message: error.message,
        }),
    }
}

pub trait IntoResourceOutcomeDto {
    fn into_resource_outcome(self) -> ResourceOutcome;
}

impl IntoResourceOutcomeDto for CreateTopicOutcome {
    fn into_resource_outcome(self) -> ResourceOutcome {
        kafka_error_outcome(self.name, self.error)
    }
}

impl IntoResourceOutcomeDto for DeleteTopicOutcome {
    fn into_resource_outcome(self) -> ResourceOutcome {
        kafka_error_outcome(self.name, self.error)
    }
}

impl IntoResourceOutcomeDto for CreatePartitionsOutcome {
    fn into_resource_outcome(self) -> ResourceOutcome {
        kafka_error_outcome(self.name, self.error)
    }
}

impl IntoResourceOutcomeDto for AlterConfigsOutcome {
    fn into_resource_outcome(self) -> ResourceOutcome {
        kafka_error_outcome(self.topic, self.error)
    }
}

impl IntoResourceOutcomeDto for CreateAclOutcome {
    fn into_resource_outcome(self) -> ResourceOutcome {
        kafka_error_outcome("acl".to_string(), self.error)
    }
}

impl IntoResourceOutcomeDto for DeleteAclFilterOutcome {
    fn into_resource_outcome(self) -> ResourceOutcome {
        kafka_error_outcome("acl-filter".to_string(), self.error)
    }
}

impl IntoResourceOutcomeDto for ScramUserOutcome {
    fn into_resource_outcome(self) -> ResourceOutcome {
        kafka_error_outcome(self.username, self.error)
    }
}

impl IntoResourceOutcomeDto for AlterReplicaLogDirOutcome {
    fn into_resource_outcome(self) -> ResourceOutcome {
        kafka_error_outcome(format!("{}-{}", self.topic, self.partition), self.error)
    }
}

#[must_use]
pub fn singleton_quota_config(quota_type: String, value: f64) -> UserQuotaConfig {
    BTreeMap::from([(quota_type, value)])
}
