//! `OffsetFetch` (`api_key=9`). Reads from `Group.committed_offsets`.
//!
//! For v0-v7 the request carries the legacy single-group fields:
//! `group_id` + `topics: Option<Vec<OffsetFetchRequestTopic>>`. v8+ moved
//! to a per-group array; for the MVP we ignore the `groups` array and only
//! serve the legacy single-group shape.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::offset_fetch_request::OffsetFetchRequest;
use crabka_protocol::owned::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = OffsetFetchRequest::decode(&mut cur, version)?;

        let handle = group_manager.get_or_create(&req.group_id);
        let g = handle.state.lock().await;

        // A `None` `topics` field (v ≥ 2) is the "fetch all" sentinel:
        // return every committed offset stored for this group.
        let topics_out: Vec<OffsetFetchResponseTopic> = if req.topics.is_none() {
            // Aggregate all committed offsets grouped by topic name.
            let mut by_topic: std::collections::HashMap<String, Vec<OffsetFetchResponsePartition>> =
                std::collections::HashMap::new();
            for ((topic, pid), entry) in &g.committed_offsets {
                by_topic
                    .entry(topic.clone())
                    .or_default()
                    .push(OffsetFetchResponsePartition {
                        partition_index: *pid,
                        committed_offset: entry.offset,
                        committed_leader_epoch: entry.leader_epoch,
                        metadata: Some(entry.metadata.clone()),
                        error_code: codes::NONE,
                        ..Default::default()
                    });
            }
            by_topic
                .into_iter()
                .map(|(name, partitions)| OffsetFetchResponseTopic {
                    name,
                    partitions,
                    ..Default::default()
                })
                .collect()
        } else {
            let req_topics = req.topics.as_deref().unwrap_or(&[]);
            req_topics
                .iter()
                .map(|t| {
                    let partitions = t
                        .partition_indexes
                        .iter()
                        .map(
                            |&pid| match g.committed_offsets.get(&(t.name.clone(), pid)) {
                                Some(entry) => OffsetFetchResponsePartition {
                                    partition_index: pid,
                                    committed_offset: entry.offset,
                                    committed_leader_epoch: entry.leader_epoch,
                                    metadata: Some(entry.metadata.clone()),
                                    error_code: codes::NONE,
                                    ..Default::default()
                                },
                                None => OffsetFetchResponsePartition {
                                    partition_index: pid,
                                    committed_offset: -1,
                                    committed_leader_epoch: -1,
                                    metadata: None,
                                    error_code: codes::NONE,
                                    ..Default::default()
                                },
                            },
                        )
                        .collect();
                    OffsetFetchResponseTopic {
                        name: t.name.clone(),
                        partitions,
                        ..Default::default()
                    }
                })
                .collect()
        };

        let resp = OffsetFetchResponse {
            topics: topics_out,
            error_code: codes::NONE,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
