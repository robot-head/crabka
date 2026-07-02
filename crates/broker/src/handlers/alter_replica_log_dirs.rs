//! `AlterReplicaLogDirs` (`api_key=34`, KIP-113). Intra-broker
//! log-directory reassignment: move a hosted replica from one of this
//! broker's configured `log.dirs` to another, without re-replicating
//! from the leader. Backs `kafka-log-dirs --alter` and the
//! `--reassignment-json-file` log-dir overrides of
//! `kafka-reassign-partitions`.
//!
//! The handler immediately validates inputs, kicks off a per-move
//! background replicator task via [`crate::future_log::start_move`],
//! and returns success. The actual data copy + atomic dir-rename
//! happens in the background; clients poll `DescribeLogDirs` and
//! watch `is_future_key` flip from `true` to `false` to detect
//! completion.
//!
//! Authorisation (Cluster.Alter) is enforced at
//! [`crate::network::dispatch::handle_alter_replica_log_dirs_frame`] —
//! this handler runs only after the principal has been authorized.

use std::{collections::BTreeMap, path::PathBuf};

use bytes::{Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
        alter_replica_log_dirs_response::{
            AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
            AlterReplicaLogDirsResponse,
        },
    },
};
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    future_log::{self, MoveError},
};

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    let future_logs = broker.future_logs.clone();
    let all_log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;

        // (topic, partition) → error code. The wire format lets a
        // client list the same partition under multiple target dirs;
        // Apache Kafka treats the LAST occurrence as authoritative,
        // so we overwrite as we iterate.
        let mut per_partition: BTreeMap<(String, i32), i16> = BTreeMap::new();

        for dir in req.dirs {
            let target_path = PathBuf::from(&dir.path);
            for topic in dir.topics {
                for partition_index in topic.partitions {
                    let code = match future_log::start_move(
                        &partitions,
                        &future_logs,
                        &all_log_dirs,
                        &log_config,
                        &topic.name,
                        partition_index,
                        &target_path,
                    ) {
                        Ok(()) => codes::NONE,
                        Err(MoveError::LogDirNotFound) => codes::LOG_DIR_NOT_FOUND,
                        Err(MoveError::ReplicaNotAvailable) => codes::REPLICA_NOT_AVAILABLE,
                        Err(MoveError::AlreadyMoving | MoveError::Storage(_)) => {
                            codes::KAFKA_STORAGE_ERROR
                        }
                    };
                    per_partition.insert((topic.name.clone(), partition_index), code);
                }
            }
        }

        // Group per-partition results back into the response's
        // per-topic shape.
        let mut by_topic: BTreeMap<String, Vec<AlterReplicaLogDirPartitionResult>> =
            BTreeMap::new();
        for ((topic, partition), code) in per_partition {
            by_topic
                .entry(topic)
                .or_default()
                .push(AlterReplicaLogDirPartitionResult {
                    partition_index: partition,
                    error_code: code,
                    ..Default::default()
                });
        }

        let results: Vec<_> = by_topic
            .into_iter()
            .map(|(name, partitions)| AlterReplicaLogDirTopicResult {
                topic_name: name,
                partitions,
                ..Default::default()
            })
            .collect();

        let resp = AlterReplicaLogDirsResponse {
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_protocol::owned::alter_replica_log_dirs_request::{
        AlterReplicaLogDir, AlterReplicaLogDirTopic,
    };

    use super::*;

    crate::test_support::codec_helpers!(AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse);

    async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|_cfg| {}).await
    }

    #[tokio::test]
    async fn handle_preserves_unknown_target_response_shape() {
        let version = 2;
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let req = AlterReplicaLogDirsRequest {
            dirs: vec![AlterReplicaLogDir {
                path: "/tmp/crabka-missing-log-dir".into(),
                topics: vec![AlterReplicaLogDirTopic {
                    name: "orders".into(),
                    partitions: vec![7],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_bytes = encode_request(&req, version);

        let bytes = handle(&broker, version, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = AlterReplicaLogDirsResponse {
            throttle_time_ms: 0,
            results: vec![AlterReplicaLogDirTopicResult {
                topic_name: "orders".to_string(),
                partitions: vec![AlterReplicaLogDirPartitionResult {
                    partition_index: 7,
                    error_code: codes::LOG_DIR_NOT_FOUND,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
