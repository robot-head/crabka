//! KIP-113 admin RPCs: `AlterReplicaLogDirs` (`api_key` 34) and
//! `DescribeLogDirs` (`api_key` 35).
//!
//! Both target the broker the connection is open against — these are
//! per-broker calls, so the admin client does NOT do a controller
//! retry on `NOT_CONTROLLER` (the request doesn't hit the controller).

use std::collections::BTreeMap;

use crabka_protocol::owned::{
    alter_replica_log_dirs_request::{
        AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
    },
    describe_log_dirs_request::{DescribableLogDirTopic, DescribeLogDirsRequest},
};

use crate::{AdminClient, AdminError, KafkaError, kafka_error_if};

/// One row of an `AlterReplicaLogDirs` result.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirOutcome {
    pub topic: String,
    pub partition: i32,
    pub error: Option<KafkaError>,
}

/// One log dir from a `DescribeLogDirs` response.
#[derive(Debug, Clone)]
pub struct LogDirInfo {
    pub log_dir: String,
    pub error: Option<KafkaError>,
    pub topics: Vec<LogDirTopicInfo>,
}

#[derive(Debug, Clone)]
pub struct LogDirTopicInfo {
    pub name: String,
    pub partitions: Vec<LogDirPartitionInfo>,
}

#[derive(Debug, Clone)]
pub struct LogDirPartitionInfo {
    pub partition_index: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future_key: bool,
}

impl AdminClient {
    /// `AlterReplicaLogDirs` (KIP-113): move replicas between local
    /// `log.dirs` on this broker.
    ///
    /// `assignments` maps each target absolute directory path to the
    /// `(topic, [partition])` pairs that should be moved into it.
    pub async fn alter_replica_log_dirs(
        &mut self,
        assignments: &BTreeMap<String, Vec<(String, Vec<i32>)>>,
    ) -> Result<Vec<AlterReplicaLogDirOutcome>, AdminError> {
        let dirs = assignments
            .iter()
            .map(|(path, topics)| AlterReplicaLogDir {
                path: path.clone(),
                topics: topics
                    .iter()
                    .map(|(name, partitions)| AlterReplicaLogDirTopic {
                        name: name.clone(),
                        partitions: partitions.clone(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();
        let req = AlterReplicaLogDirsRequest {
            dirs,
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;

        let mut out = Vec::new();
        for topic in resp.results {
            for p in topic.partitions {
                let error = kafka_error_if(p.error_code, None);
                out.push(AlterReplicaLogDirOutcome {
                    topic: topic.topic_name.clone(),
                    partition: p.partition_index,
                    error,
                });
            }
        }
        Ok(out)
    }

    /// `DescribeLogDirs` (KIP-113): list every configured `log.dir` on
    /// this broker, with the partitions each holds (current and
    /// in-progress future logs). Pass `None` to fetch all partitions
    /// or `Some` with topic → partitions filter (empty inner vec means
    /// all partitions of that topic).
    pub async fn describe_log_dirs(
        &mut self,
        filter: Option<&BTreeMap<String, Vec<i32>>>,
    ) -> Result<Vec<LogDirInfo>, AdminError> {
        let topics = filter.map(|f| {
            f.iter()
                .map(|(name, partitions)| DescribableLogDirTopic {
                    topic: name.clone(),
                    partitions: partitions.clone(),
                    ..Default::default()
                })
                .collect()
        });
        let req = DescribeLogDirsRequest {
            topics,
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;

        let mut out = Vec::new();
        for result in resp.results {
            let error = kafka_error_if(result.error_code, None);
            let topics = result
                .topics
                .into_iter()
                .map(|t| LogDirTopicInfo {
                    name: t.name,
                    partitions: t
                        .partitions
                        .into_iter()
                        .map(|p| LogDirPartitionInfo {
                            partition_index: p.partition_index,
                            partition_size: p.partition_size,
                            offset_lag: p.offset_lag,
                            is_future_key: p.is_future_key,
                        })
                        .collect(),
                })
                .collect();
            out.push(LogDirInfo {
                log_dir: result.log_dir,
                error,
                topics,
            });
        }
        Ok(out)
    }
}
