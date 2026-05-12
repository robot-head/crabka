//! `CreateTopics` (`api_key=19`). Routes through `Controller::submit_change`
//! so every topic/partition creation goes through the metadata quorum before
//! the partition directories are materialized on disk.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use crabka_protocol::owned::create_topics_request::CreateTopicsRequest;
use crabka_protocol::owned::create_topics_response::{CreatableTopicResult, CreateTopicsResponse};
use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;
use uuid::Uuid;

use crate::broker::{Broker, spawn_partition};
use crate::codes;
use crate::error::BrokerError;
use crate::log_dir;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let log_dir = broker.config.log_dir.clone();
    let log_config = broker.config.log_config.clone();
    let partitions_map = broker.partitions.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = CreateTopicsRequest::decode(&mut cur, version)?;
        let mut results: Vec<CreatableTopicResult> = Vec::with_capacity(req.topics.len());

        for topic_req in req.topics {
            let name = topic_req.name.clone();
            let partition_count = topic_req.num_partitions;
            let replication_factor = topic_req.replication_factor;
            let topic_id = Uuid::new_v4();

            // Build the batch: one TopicRecord + N PartitionRecords.
            let mut records = Vec::with_capacity(1 + partition_count.max(0) as usize);
            records.push(MetadataRecord::V1Topic(TopicRecord {
                name: name.clone(),
                topic_id,
                partitions: partition_count,
                replication_factor,
            }));
            for p in 0..partition_count.max(0) {
                records.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: name.clone(),
                    partition: p,
                    leader: node_id,
                    replicas: vec![node_id],
                    isr: vec![node_id],
                }));
            }

            let error_code = match controller.submit_change(records).await {
                Ok(()) => {
                    // Committed to quorum — materialize on-disk partitions.
                    for p in 0..partition_count.max(0) {
                        let dir = log_dir::partition_dir(&log_dir, &name, p);
                        match std::fs::create_dir_all(&dir)
                            .map_err(BrokerError::from)
                            .and_then(|()| {
                                crabka_log::Log::open(&dir, log_config.clone())
                                    .map_err(BrokerError::from)
                            }) {
                            Ok(log) => {
                                let part = spawn_partition(name.clone(), p, log);
                                partitions_map.insert((name.clone(), p), part);
                            }
                            Err(e) => {
                                tracing::error!(
                                    topic = %name,
                                    partition = p,
                                    error = %e,
                                    "CreateTopics: disk failure after quorum commit"
                                );
                                // Quorum already committed — we cannot roll back.
                                // Partition will be recovered on next broker restart.
                            }
                        }
                    }
                    codes::NONE
                }
                Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => {
                    codes::TOPIC_ALREADY_EXISTS
                }
                Err(RaftError::NotLeader { .. }) | Err(RaftError::LeaderUnknown) => {
                    codes::NOT_CONTROLLER
                }
                Err(e) => {
                    tracing::error!(topic = %name, error = %e, "CreateTopics submit_change failed");
                    codes::UNKNOWN_SERVER_ERROR
                }
            };

            // Convert uuid::Uuid → crabka_protocol::primitives::uuid::Uuid.
            let proto_uuid = ProtoUuid(topic_id.into_bytes());

            let mut result = CreatableTopicResult {
                name,
                topic_id: proto_uuid,
                error_code,
                error_message: None,
                ..Default::default()
            };

            if error_code == codes::NONE {
                result.num_partitions = partition_count;
                result.replication_factor = replication_factor;
                // KIP-525 (v5+): return an empty configs list to satisfy
                // clients that unconditionally call `configs().stream()`.
                result.configs = Some(Vec::new());
            }
            results.push(result);
        }

        let resp = CreateTopicsResponse {
            topics: results,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
