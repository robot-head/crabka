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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use bytes::{Buf, BytesMut};
    use crabka_client_core::MockBroker;
    use crabka_protocol::{
        Decode, Encode,
        owned::{
            alter_replica_log_dirs_request,
            alter_replica_log_dirs_response::{
                AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                AlterReplicaLogDirsResponse,
            },
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            describe_log_dirs_request,
            describe_log_dirs_response::{
                DescribeLogDirsPartition, DescribeLogDirsResponse, DescribeLogDirsResult,
                DescribeLogDirsTopic,
            },
        },
    };

    use super::*;

    fn encode_v0(resp: &impl Encode) -> Vec<u8> {
        encode_at(resp, 0)
    }

    fn encode_at(resp: &impl Encode, version: i16) -> Vec<u8> {
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn api_versions_response(api_key: i16, version: i16) -> Vec<u8> {
        encode_v0(&ApiVersionsResponse {
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 0,
                    ..Default::default()
                },
                ApiVersion {
                    api_key,
                    min_version: version,
                    max_version: version,
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
    }

    fn request_body_after_header(mut body: &[u8], flexible_header: bool) -> &[u8] {
        let client_id_len = body.get_i16();
        assert2::assert!(client_id_len >= 0);
        body.advance(usize::try_from(client_id_len).expect("client id length is non-negative"));
        if flexible_header {
            assert2::assert!(body.get_u8() == 0);
        }
        body
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_replica_log_dirs_maps_non_empty_partition_results() {
        let seen_request = Arc::new(Mutex::new(None));
        let captured_request = Arc::clone(&seen_request);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(
                    alter_replica_log_dirs_request::API_KEY,
                    1,
                ));
            }
            if api_key == alter_replica_log_dirs_request::API_KEY {
                let mut body = request_body_after_header(
                    body,
                    version >= alter_replica_log_dirs_request::FLEXIBLE_MIN,
                );
                let request = AlterReplicaLogDirsRequest::decode(&mut body, version)
                    .expect("alter log dirs request decodes");
                assert2::assert!(body.is_empty());
                *captured_request.lock().expect("request capture lock") = Some(request);
                return Some(encode_at(
                    &AlterReplicaLogDirsResponse {
                        results: vec![AlterReplicaLogDirTopicResult {
                            topic_name: "orders".into(),
                            partitions: vec![AlterReplicaLogDirPartitionResult {
                                partition_index: 2,
                                error_code: 56,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    1,
                ));
            }
            None
        })
        .await;
        let mut admin = AdminClient::connect(&[mock.addr.to_string()])
            .await
            .expect("admin connects to mock broker");
        let assignments = BTreeMap::from([(
            "/var/lib/kafka-a".to_string(),
            vec![("orders".to_string(), vec![2])],
        )]);

        let outcomes = admin
            .alter_replica_log_dirs(&assignments)
            .await
            .expect("alter log dirs response maps");

        let error = outcomes[0]
            .error
            .as_ref()
            .expect("broker error is surfaced");
        assert2::assert!(
            (
                outcomes.len(),
                &outcomes[0].topic,
                outcomes[0].partition,
                error.code,
                &error.message
            ) == (1, &"orders".to_string(), 2, 56, &None)
        );
        let request = seen_request
            .lock()
            .expect("request capture lock")
            .take()
            .expect("alter log dirs request was captured");
        assert2::assert!(
            request
                == AlterReplicaLogDirsRequest {
                    dirs: vec![AlterReplicaLogDir {
                        path: "/var/lib/kafka-a".into(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![2],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
        );
        mock.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_log_dirs_maps_non_empty_directory_tree() {
        let seen_request = Arc::new(Mutex::new(None));
        let captured_request = Arc::clone(&seen_request);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(describe_log_dirs_request::API_KEY, 1));
            }
            if api_key == describe_log_dirs_request::API_KEY {
                let mut body = request_body_after_header(
                    body,
                    version >= describe_log_dirs_request::FLEXIBLE_MIN,
                );
                let request = DescribeLogDirsRequest::decode(&mut body, version)
                    .expect("describe log dirs request decodes");
                assert2::assert!(body.is_empty());
                *captured_request.lock().expect("request capture lock") = Some(request);
                return Some(encode_at(
                    &DescribeLogDirsResponse {
                        results: vec![DescribeLogDirsResult {
                            log_dir: "/data/kafka".into(),
                            topics: vec![DescribeLogDirsTopic {
                                name: "orders".into(),
                                partitions: vec![DescribeLogDirsPartition {
                                    partition_index: 4,
                                    partition_size: 123,
                                    offset_lag: 7,
                                    is_future_key: true,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    1,
                ));
            }
            None
        })
        .await;
        let mut admin = AdminClient::connect(&[mock.addr.to_string()])
            .await
            .expect("admin connects to mock broker");
        let filter = BTreeMap::from([("orders".to_string(), vec![4])]);

        let dirs = admin
            .describe_log_dirs(Some(&filter))
            .await
            .expect("describe log dirs response maps");

        let partition = &dirs[0].topics[0].partitions[0];
        assert2::assert!(
            (
                dirs.len(),
                dirs[0].log_dir.as_str(),
                dirs[0].error.as_ref(),
                dirs[0].topics.len(),
                dirs[0].topics[0].name.as_str(),
                partition.partition_index,
                partition.partition_size,
                partition.offset_lag,
                partition.is_future_key,
            ) == (1, "/data/kafka", None, 1, "orders", 4, 123, 7, true)
        );
        let request = seen_request
            .lock()
            .expect("request capture lock")
            .take()
            .expect("describe log dirs request was captured");
        assert2::assert!(
            request
                == DescribeLogDirsRequest {
                    topics: Some(vec![DescribableLogDirTopic {
                        topic: "orders".into(),
                        partitions: vec![4],
                        ..Default::default()
                    }]),
                    ..Default::default()
                }
        );
        mock.stop();
    }
}
