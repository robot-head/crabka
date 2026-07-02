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

use crabka_protocol::{
    owned::{
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics,
        },
        offset_fetch_response::OffsetFetchResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};

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
/// committed_offset, committed_leader_epoch)` tuples. v8+ data lives in
/// `groups`; v0-7 in `topics`. At v10 the per-topic name is empty, so
/// resolve it from `topic_id` via `id_to_name`.
pub(crate) fn parse_offset_fetch(
    resp: &OffsetFetchResponse,
    id_to_name: &HashMap<WireUuid, String>,
) -> Vec<(String, i32, i64, i32)> {
    let mut out = Vec::new();
    if resp.groups.is_empty() {
        for t in &resp.topics {
            for p in &t.partitions {
                out.push((
                    t.name.clone(),
                    p.partition_index,
                    p.committed_offset,
                    p.committed_leader_epoch,
                ));
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
                    out.push((
                        name.clone(),
                        p.partition_index,
                        p.committed_offset,
                        p.committed_leader_epoch,
                    ));
                }
            }
        }
    }
    out
}

/// Build the `topics` for an `OffsetCommit`, tagging each with its `topic_id`
/// (required at v10, where the wire drops the topic name). The name is kept
/// for v0-9. `offsets` maps `(topic, partition)` to
/// `(committed_offset, committed_leader_epoch)`.
pub(crate) fn build_commit_topics(
    offsets: HashMap<(String, i32), (i64, i32)>,
    topic_ids: &HashMap<String, WireUuid>,
) -> Vec<OffsetCommitRequestTopic> {
    let mut by_topic: HashMap<String, Vec<(i32, i64, i32)>> = HashMap::new();
    for ((t, p), (off, epoch)) in offsets {
        by_topic.entry(t).or_default().push((p, off, epoch));
    }
    by_topic
        .into_iter()
        .map(|(name, parts)| OffsetCommitRequestTopic {
            topic_id: topic_ids.get(&name).copied().unwrap_or_default(),
            name,
            partitions: parts
                .into_iter()
                .map(|(p, off, epoch)| OffsetCommitRequestPartition {
                    partition_index: p,
                    committed_offset: off,
                    committed_leader_epoch: epoch,
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

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
            OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
        },
    };

    use super::*;

    fn id(n: u8) -> WireUuid {
        let mut b = [0u8; 16];
        b[15] = n;
        WireUuid(b)
    }

    #[test]
    fn build_offset_fetch_populates_legacy_and_groups() {
        let mut by_topic = HashMap::new();
        by_topic.insert("t".to_string(), vec![0, 1]);
        let mut ids = HashMap::new();
        ids.insert("t".to_string(), id(7));

        let req = build_offset_fetch("g", &by_topic, &ids);
        // Legacy single-group fields (v0-7) AND v8+ groups[] with topic_id (v10).
        assert!(
            req == OffsetFetchRequest {
                group_id: "g".to_string(),
                topics: Some(vec![OffsetFetchRequestTopic {
                    name: "t".to_string(),
                    partition_indexes: vec![0, 1],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }]),
                groups: vec![OffsetFetchRequestGroup {
                    group_id: "g".to_string(),
                    member_id: None,
                    member_epoch: -1,
                    topics: Some(vec![OffsetFetchRequestTopics {
                        name: "t".to_string(),
                        topic_id: id(7),
                        partition_indexes: vec![0, 1],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }]),
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                require_stable: false,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn build_offset_fetch_defaults_topic_id_when_unknown() {
        let mut by_topic = HashMap::new();
        by_topic.insert("t".to_string(), vec![0]);
        let req = build_offset_fetch("g", &by_topic, &HashMap::new());
        let gtops = req.groups[0].topics.as_ref().unwrap();
        assert!(gtops[0].topic_id == WireUuid::ZERO);
    }

    #[test]
    fn parse_offset_fetch_reads_groups_with_name() {
        let resp = OffsetFetchResponse {
            groups: vec![OffsetFetchResponseGroup {
                group_id: "g".into(),
                topics: vec![OffsetFetchResponseTopics {
                    name: "t".into(),
                    topic_id: id(7),
                    partitions: vec![OffsetFetchResponsePartitions {
                        partition_index: 3,
                        committed_offset: 42,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = parse_offset_fetch(&resp, &HashMap::new());
        assert!(out == vec![("t".to_string(), 3, 42, -1)]);
    }

    #[test]
    fn parse_offset_fetch_resolves_topic_id_when_name_empty() {
        // v10: response topic carries only topic_id; name resolved via map.
        let resp = OffsetFetchResponse {
            groups: vec![OffsetFetchResponseGroup {
                group_id: "g".into(),
                topics: vec![OffsetFetchResponseTopics {
                    name: String::new(),
                    topic_id: id(9),
                    partitions: vec![OffsetFetchResponsePartitions {
                        partition_index: 0,
                        committed_offset: 5,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut id_map = HashMap::new();
        id_map.insert(id(9), "named".to_string());
        let out = parse_offset_fetch(&resp, &id_map);
        assert!(out == vec![("named".to_string(), 0, 5, -1)]);
    }

    #[test]
    fn parse_offset_fetch_falls_back_to_legacy_topics() {
        // v0-7: no groups, data in the top-level `topics` field.
        let resp = OffsetFetchResponse {
            topics: vec![OffsetFetchResponseTopic {
                name: "legacy".into(),
                partitions: vec![OffsetFetchResponsePartition {
                    partition_index: 1,
                    committed_offset: 11,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = parse_offset_fetch(&resp, &HashMap::new());
        assert!(out == vec![("legacy".to_string(), 1, 11, -1)]);
    }

    #[test]
    fn build_commit_topics_tags_topic_id() {
        let mut offsets = HashMap::new();
        offsets.insert(("t".to_string(), 3), (100, 5));
        let mut ids = HashMap::new();
        ids.insert("t".to_string(), id(7));
        let topics = build_commit_topics(offsets, &ids);
        assert!(
            topics
                == vec![OffsetCommitRequestTopic {
                    name: "t".to_string(),
                    topic_id: id(7),
                    partitions: vec![OffsetCommitRequestPartition {
                        partition_index: 3,
                        committed_offset: 100,
                        committed_leader_epoch: 5,
                        committed_metadata: Some(String::new()),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }]
        );

        // Missing id → ZERO default.
        let mut o2 = HashMap::new();
        o2.insert(("u".to_string(), 0), (1, -1));
        let t2 = build_commit_topics(o2, &HashMap::new());
        assert!(t2[0].topic_id == WireUuid::ZERO);
    }

    #[test]
    fn id_to_name_inverts_the_map() {
        let mut ids = HashMap::new();
        ids.insert("t".to_string(), id(7));
        let inv = id_to_name(&ids);
        assert!(inv.get(&id(7)) == Some(&"t".to_string()));
    }
}
