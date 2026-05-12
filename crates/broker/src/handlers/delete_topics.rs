//! `DeleteTopics` (`api_key=20`). Routes through `Controller::submit_change`
//! so every topic deletion is recorded in the metadata quorum before the
//! partition dirs and in-memory state are torn down.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{DeleteTopicRecord, MetadataRecord};
use crabka_protocol::owned::delete_topics_request::DeleteTopicsRequest;
use crabka_protocol::owned::delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse};
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::broker::Broker;
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
    let partitions = broker.partitions.clone();
    let log_dir = broker.config.log_dir.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DeleteTopicsRequest::decode(&mut cur, version)?;

        // v0-5: `topic_names: Vec<String>` (topic_id not present).
        // v6+:  `topics: Vec<DeleteTopicState>` with optional name + topic_id.
        //
        // Collect (name, topic_id_bytes) pairs. If the client sent only a
        // topic_id (name is None), resolve the name from the current image.
        let mut name_list: Vec<Option<String>> = Vec::new();
        if req.topic_names.is_empty() {
            let image = controller.current_image();
            for state in &req.topics {
                if let Some(ref n) = state.name {
                    name_list.push(Some(n.clone()));
                } else {
                    // name is absent — resolve by topic_id from the image.
                    let found = image
                        .topics()
                        .find(|t| t.topic_id.into_bytes() == state.topic_id.0)
                        .map(|t| t.name.clone());
                    name_list.push(found);
                }
            }
        } else {
            for n in &req.topic_names {
                name_list.push(Some(n.clone()));
            }
        }

        let mut results: Vec<DeletableTopicResult> = Vec::with_capacity(name_list.len());

        for name_opt in name_list {
            let Some(name) = name_opt else {
                // topic_id not found in image — unknown topic.
                results.push(DeletableTopicResult {
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    ..Default::default()
                });
                continue;
            };

            let res = controller
                .submit_change(vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                    name: name.clone(),
                })])
                .await;

            let error_code = match res {
                Ok(()) => {
                    // Committed to quorum — tear down in-memory state and dirs.
                    let keys: Vec<(String, i32)> = partitions
                        .iter()
                        .map(|e| e.key().clone())
                        .filter(|(t, _)| t == &name)
                        .collect();
                    for k in keys {
                        partitions.remove(&k);
                        let dir = log_dir::partition_dir(&log_dir, &k.0, k.1);
                        let _ = std::fs::remove_dir_all(dir);
                    }
                    codes::NONE
                }
                Err(RaftError::Metadata(crabka_metadata::MetadataError::UnknownTopic(_))) => {
                    codes::UNKNOWN_TOPIC_OR_PARTITION
                }
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    codes::NOT_CONTROLLER
                }
                Err(e) => {
                    tracing::error!(topic = %name, error = %e, "DeleteTopics submit_change failed");
                    codes::UNKNOWN_SERVER_ERROR
                }
            };

            results.push(DeletableTopicResult {
                name: Some(name),
                error_code,
                ..Default::default()
            });
        }

        let resp = DeleteTopicsResponse {
            responses: results,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
