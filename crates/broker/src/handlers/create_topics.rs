//! `CreateTopics` (`api_key=19`). Mutates the metadata image, creates each
//! partition's directory + `crabka-log` Log, and spawns its writer task.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::create_topics_request::CreateTopicsRequest;
use crabka_protocol::owned::create_topics_response::{CreatableTopicResult, CreateTopicsResponse};
use crabka_protocol::{Decode, Encode};

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
    // We need the broker for state mutation, but the BoxFuture lifetime is
    // 'static — clone what we need.
    let log_dir = broker.config.log_dir.clone();
    let log_config = broker.config.log_config.clone();
    let broker_id = broker.config.broker_id;
    let metadata = broker.metadata.clone();
    let partitions = broker.partitions.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = CreateTopicsRequest::decode(&mut cur, version)?;

        let mut results: Vec<CreatableTopicResult> = Vec::with_capacity(req.topics.len());

        for topic_req in req.topics {
            let name = topic_req.name.clone();
            let partition_count = topic_req.num_partitions;
            let mut result = CreatableTopicResult {
                name: name.clone(),
                ..Default::default()
            };

            if partition_count <= 0 {
                result.error_code = codes::INVALID_PARTITIONS;
                results.push(result);
                continue;
            }

            // Mutate metadata first (cheap rollback if disk fails next).
            let inserted = {
                let mut meta = metadata.write().expect("metadata poisoned");
                meta.insert_topic(&name, partition_count, broker_id)
            };
            if !inserted {
                result.error_code = codes::TOPIC_ALREADY_EXISTS;
                results.push(result);
                continue;
            }

            // Create each partition on disk and spawn its writer.
            let mut create_err: Option<BrokerError> = None;
            for partition_id in 0..partition_count {
                let dir = log_dir::partition_dir(&log_dir, &name, partition_id);
                match std::fs::create_dir_all(&dir)
                    .map_err(BrokerError::from)
                    .and_then(|()| {
                        crabka_log::Log::open(&dir, log_config.clone()).map_err(BrokerError::from)
                    }) {
                    Ok(log) => {
                        let part = spawn_partition(name.clone(), partition_id, log);
                        partitions.insert((name.clone(), partition_id), part);
                    }
                    Err(e) => {
                        create_err = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = create_err {
                tracing::error!(
                    topic = %name,
                    error = %e,
                    "create_topics: disk failure; rolling back metadata"
                );
                let mut meta = metadata.write().expect("metadata poisoned");
                meta.remove_topic(&name);
                // Best-effort cleanup of partition dirs we already created.
                for partition_id in 0..partition_count {
                    let _ = std::fs::remove_dir_all(log_dir::partition_dir(
                        &log_dir,
                        &name,
                        partition_id,
                    ));
                    partitions.remove(&(name.clone(), partition_id));
                }
                result.error_code = codes::UNKNOWN_SERVER_ERROR;
                results.push(result);
                continue;
            }

            result.error_code = codes::NONE;
            result.num_partitions = partition_count;
            result.replication_factor = 1;
            // KIP-525 (v5+): client unconditionally calls `configs().stream()`,
            // which NPEs if we leave this as `None`. Return an empty list to
            // signal "no topic-level overrides" while keeping the call safe.
            result.configs = Some(Vec::new());
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
