//! KIP-516 offset wire-shape helpers.
//!
//! The broker advertises `OffsetCommit` v10 and `OffsetFetch` v8+, so the
//! client negotiates up to those versions. At v8+ `OffsetFetch` carries a
//! per-group `groups[]` array (the legacy `group_id` + `topics` fields are
//! v0-7 only), and at v10 both APIs key topics by `topic_id` instead of name.
//!
//! These builders populate BOTH the legacy and the new fields — the codegen
//! encodes only the set valid for the negotiated version, so one request
//! works regardless of what the broker negotiated. The parser flattens an
//! `OffsetFetch` response across either shape, resolving `topic_id` back to a
//! name (the name is dropped from the wire at v10).

use std::collections::HashMap;

use crabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use crabka_protocol::owned::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use crabka_protocol::owned::offset_fetch_response::OffsetFetchResponse;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

/// Build an `OffsetFetch` request covering `by_topic`, valid at any negotiated
/// version. Legacy `group_id`/`topics` (v0-7) and the v8+ `groups[]` array
/// (with `topic_id` for v10) are both populated.
pub(crate) fn build_offset_fetch(
    group_id: &str,
    by_topic: &HashMap<String, Vec<i32>>,
    topic_ids: &HashMap<String, WireUuid>,
) -> OffsetFetchRequest {
    let legacy_topics: Vec<OffsetFetchRequestTopic> = by_topic
        .iter()
        .map(|(name, parts)| OffsetFetchRequestTopic {
            name: name.clone(),
            partition_indexes: parts.clone(),
            ..Default::default()
        })
        .collect();
    let group_topics: Vec<OffsetFetchRequestTopics> = by_topic
        .iter()
        .map(|(name, parts)| OffsetFetchRequestTopics {
            name: name.clone(),
            topic_id: topic_ids.get(name).copied().unwrap_or_default(),
            partition_indexes: parts.clone(),
            ..Default::default()
        })
        .collect();
    OffsetFetchRequest {
        group_id: group_id.to_string(),
        topics: Some(legacy_topics),
        groups: vec![OffsetFetchRequestGroup {
            group_id: group_id.to_string(),
            topics: Some(group_topics),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Flatten an `OffsetFetch` response into `(topic_name, partition,
/// committed_offset)` triples. v8+ data lives in `groups`; v0-7 in `topics`.
/// At v10 the per-topic name is empty, so resolve it from `topic_id` via
/// `id_to_name`.
pub(crate) fn parse_offset_fetch(
    resp: &OffsetFetchResponse,
    id_to_name: &HashMap<WireUuid, String>,
) -> Vec<(String, i32, i64)> {
    let mut out = Vec::new();
    if resp.groups.is_empty() {
        for t in &resp.topics {
            for p in &t.partitions {
                out.push((t.name.clone(), p.partition_index, p.committed_offset));
            }
        }
    } else {
        for g in &resp.groups {
            for t in &g.topics {
                let name = if t.name.is_empty() {
                    id_to_name.get(&t.topic_id).cloned().unwrap_or_default()
                } else {
                    t.name.clone()
                };
                for p in &t.partitions {
                    out.push((name.clone(), p.partition_index, p.committed_offset));
                }
            }
        }
    }
    out
}

/// Build the `topics` for an `OffsetCommit`, tagging each with its `topic_id`
/// (required at v10, where the wire drops the topic name). The name is kept
/// for v0-9.
pub(crate) fn build_commit_topics(
    offsets: HashMap<(String, i32), i64>,
    topic_ids: &HashMap<String, WireUuid>,
) -> Vec<OffsetCommitRequestTopic> {
    let mut by_topic: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
    for ((t, p), off) in offsets {
        by_topic.entry(t).or_default().push((p, off));
    }
    by_topic
        .into_iter()
        .map(|(name, parts)| OffsetCommitRequestTopic {
            topic_id: topic_ids.get(&name).copied().unwrap_or_default(),
            name,
            partitions: parts
                .into_iter()
                .map(|(p, off)| OffsetCommitRequestPartition {
                    partition_index: p,
                    committed_offset: off,
                    committed_leader_epoch: -1,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

/// Build the `topic_id → name` reverse map from the consumer's `name →
/// topic_id` table, used to resolve `OffsetFetch` v10 responses.
pub(crate) fn id_to_name(topic_ids: &HashMap<String, WireUuid>) -> HashMap<WireUuid, String> {
    topic_ids.iter().map(|(n, id)| (*id, n.clone())).collect()
}
