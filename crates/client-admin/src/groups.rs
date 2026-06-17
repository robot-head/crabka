//! Consumer-group admin APIs: [`AdminClient::list_groups`] and
//! [`AdminClient::list_consumer_group_offsets`].
//!
//! These are thin wrappers over the `ListGroups` (`api_key`=16) and
//! `OffsetFetch` (`api_key`=9, v8+ grouped form) RPCs.
//!
//! ## `OffsetFetch` version note
//!
//! The `Connection` negotiates the highest mutually supported version (v10 at
//! the time of writing). At v10 the response encodes topics by `topic_id`
//! only — the `name` field is omitted from the wire. To return the
//! human-readable `(topic, partition) → offset` map we call `Metadata` with
//! no filter (fetch-all) immediately after and build an id→name lookup table.

use std::collections::{BTreeMap, HashMap};

use crabka_protocol::owned::{
    list_groups_request::ListGroupsRequest,
    metadata_request::MetadataRequest,
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::{AdminClient, AdminError, kafka_error_name};

/// One committed-offset row collected from an `OffsetFetch` response, retaining
/// the `topic_id` so name-less (v10) topics can be resolved via `Metadata`.
struct Entry {
    topic_name: String,
    topic_id: WireUuid,
    partition: i32,
    offset: i64,
}

impl AdminClient {
    /// Return the group-id of every consumer group known to the broker.
    pub async fn list_groups(&mut self) -> Result<Vec<String>, AdminError> {
        let req = ListGroupsRequest {
            states_filter: vec![],
            types_filter: vec![],
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::Broker {
                api: "ListGroups",
                code: resp.error_code,
                name: kafka_error_name(resp.error_code),
                message: None,
            });
        }
        Ok(resp.groups.into_iter().map(|g| g.group_id).collect())
    }

    /// Return `(topic, partition) → committed_offset` for the named group.
    ///
    /// Requests all topics/partitions (`topics: None`). Entries with a
    /// committed offset < 0 (i.e. no committed offset) are skipped.
    ///
    /// At `OffsetFetch` v10 the response carries `topic_id` instead of `name`.
    /// We resolve ids → names via a `Metadata` round-trip when any topic name
    /// in the response is empty.
    pub async fn list_consumer_group_offsets(
        &mut self,
        group: &str,
    ) -> Result<BTreeMap<(String, i32), i64>, AdminError> {
        let req = OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: group.to_string(),
                member_id: None,
                member_epoch: -1,
                topics: None,
                ..Default::default()
            }],
            require_stable: false,
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;

        // Collect raw results and any topic_ids that need name resolution.
        // At OffsetFetch v10 topic names are omitted; at v8/v9 they are present.
        let mut raw: Vec<Entry> = Vec::new();
        let mut needs_resolve = false;

        for g in resp.groups {
            if g.error_code != 0 {
                return Err(AdminError::Broker {
                    api: "OffsetFetch",
                    code: g.error_code,
                    name: kafka_error_name(g.error_code),
                    message: Some(format!("group={}", g.group_id)),
                });
            }
            for t in g.topics {
                if t.name.is_empty() && t.topic_id != WireUuid::ZERO {
                    needs_resolve = true;
                }
                for p in t.partitions {
                    if p.committed_offset >= 0 {
                        raw.push(Entry {
                            topic_name: t.name.clone(),
                            topic_id: t.topic_id,
                            partition: p.partition_index,
                            offset: p.committed_offset,
                        });
                    }
                }
            }
        }

        // If any topic came back name-less (OffsetFetch v10), fetch a Metadata
        // response and build an id→name map for resolution.
        let id_to_name: HashMap<WireUuid, String> = if needs_resolve {
            let meta = self
                .conn
                .send(MetadataRequest {
                    // `topics: None` fetches all topics.
                    topics: None,
                    allow_auto_topic_creation: false,
                    ..Default::default()
                })
                .await?;
            meta.topics
                .into_iter()
                .filter_map(|t| {
                    if t.topic_id == WireUuid::ZERO {
                        None
                    } else {
                        t.name.map(|n| (t.topic_id, n))
                    }
                })
                .collect()
        } else {
            HashMap::new()
        };

        let mut out = BTreeMap::new();
        for e in raw {
            let name = if e.topic_name.is_empty() {
                match id_to_name.get(&e.topic_id) {
                    Some(n) => n.clone(),
                    None => continue, // unknown id — skip
                }
            } else {
                e.topic_name
            };
            out.insert((name, e.partition), e.offset);
        }
        Ok(out)
    }
}
