use crabka_client_admin::{AdminClient, AdminError, LogDirInfo, TopicMetadata};

use crate::dto::{KafkaErrorDto, LogDirRow, TopicRow};

pub struct AdminFacade {
    client: AdminClient,
}

impl AdminFacade {
    #[must_use]
    pub const fn new(client: AdminClient) -> Self {
        Self { client }
    }

    pub async fn topics(&mut self) -> Result<Vec<TopicRow>, AdminError> {
        let metadata = self.client.metadata(&[]).await?;

        Ok(topic_rows(metadata))
    }

    pub async fn log_dirs(&mut self) -> Result<Vec<LogDirRow>, AdminError> {
        let log_dirs = self.client.describe_log_dirs(None).await?;

        Ok(log_dir_rows(log_dirs))
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
pub fn log_dir_rows(log_dirs: Vec<LogDirInfo>) -> Vec<LogDirRow> {
    log_dirs
        .into_iter()
        .flat_map(|log_dir| {
            let log_dir_name = log_dir.log_dir;
            let log_dir_error = log_dir.error.as_ref().map(KafkaErrorDto::from);

            log_dir.topics.into_iter().flat_map(move |topic| {
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
        })
        .collect()
}
