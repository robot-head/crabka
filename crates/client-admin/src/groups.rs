//! Consumer-group admin APIs: [`AdminClient::list_groups`] and
//! [`AdminClient::list_consumer_group_offsets`].
//!
//! These are thin wrappers over the `ListGroups` (`api_key`=16) and
//! `OffsetFetch` (`api_key`=9, v8+ grouped form) RPCs.
//!
//! ## `OffsetFetch` version note
//!
//! The `Connection` negotiates the highest mutually supported version, which
//! is v10 at the time of writing. At v10 the response encodes topics by
//! `topic_id` only, and the wire omits the `name` field. To return the
//! human-readable `(topic, partition) → offset` map, the client calls
//! `Metadata` with no filter immediately after, which fetches all topics, and
//! builds an id→name lookup table.

use std::collections::{BTreeMap, HashMap};

use crabka_protocol::{
    owned::{
        list_groups_request::ListGroupsRequest,
        metadata_request::MetadataRequest,
        offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{AdminClient, AdminError, kafka_error_name};

/// One committed-offset row collected from an `OffsetFetch` response. It keeps
/// the `topic_id`, so `Metadata` can resolve name-less v10 topics.
struct Entry {
    topic_name: String,
    topic_id: WireUuid,
    partition: i32,
    offset: i64,
}

impl AdminClient {
    /// Returns the group-id of every consumer group known to the broker.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn list_groups(&mut self) -> Result<Vec<String>, AdminError> {
        // Default request lists every group (empty state/type filters).
        let req = ListGroupsRequest::default();
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

    /// Returns `(topic, partition) → committed_offset` for the named group.
    ///
    /// The call requests all topics and partitions (`topics: None`). It skips
    /// entries with a committed offset < 0, which means no committed offset.
    ///
    /// At `OffsetFetch` v10 the response carries `topic_id` instead of
    /// `name`, so a `Metadata` round-trip resolves the topic ids to names.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;

        // Collect committed offsets, keeping each topic's id for name resolution.
        let mut raw: Vec<Entry> = Vec::new();
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

        // Build an id→name map from a `Metadata` round-trip (default request =
        // all topics). At OffsetFetch v10 the response omits names, so this is
        // how empty-named entries below recover their topic; at v8/v9 names are
        // already present and the per-entry resolution simply ignores this map.
        let id_to_name: HashMap<WireUuid, String> = {
            let meta = self.conn.send(MetadataRequest::default()).await?;
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
