//! `DeleteTopics` (`api_key=20`). Removes the metadata entry, drops every
//! partition's writer sender (which terminates the writer task), and
//! rm-rfs the partition dirs.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::delete_topics_request::DeleteTopicsRequest;
use crabka_protocol::owned::delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse};
use crabka_protocol::{Decode, Encode};

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
    let log_dir = broker.config.log_dir.clone();
    let metadata = broker.metadata.clone();
    let partitions = broker.partitions.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DeleteTopicsRequest::decode(&mut cur, version)?;

        // For v0-5 the field is `topic_names: Vec<String>`. For v6+ it's
        // `topics: Vec<DeleteTopicState>` with optional name + topic_id.
        let names: Vec<String> = if req.topic_names.is_empty() {
            req.topics.iter().filter_map(|t| t.name.clone()).collect()
        } else {
            req.topic_names.clone()
        };

        let mut results: Vec<DeletableTopicResult> = Vec::with_capacity(names.len());

        for name in names {
            let mut result = DeletableTopicResult {
                name: Some(name.clone()),
                ..Default::default()
            };

            let removed = {
                let mut meta = metadata.write().expect("metadata poisoned");
                meta.remove_topic(&name)
            };
            if !removed {
                result.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                results.push(result);
                continue;
            }

            // Drop every partition's writer sender — writer task drains
            // remaining jobs and exits.
            let keys: Vec<(String, i32)> = partitions
                .iter()
                .map(|e| e.key().clone())
                .filter(|(t, _)| t == &name)
                .collect();
            for k in keys {
                partitions.remove(&k);
                // Best-effort dir cleanup.
                let dir = log_dir::partition_dir(&log_dir, &k.0, k.1);
                let _ = std::fs::remove_dir_all(dir);
            }
            results.push(result);
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
