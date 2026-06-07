//! `StreamsGroupDescribe` (`api_key` 89) — KIP-1071. Returns one
//! `DescribedGroup` per requested `group_id`, rendered from the streams actor's
//! `Describe` view.
//!
//! Mirrors the KIP-848 consumer-group describe handler
//! ([`super::consumer_group_describe`]): a plain 4-arg handler (NOT inline
//! intercepted) gated on the same `streams.version` feature + `streams_group`
//! config as the heartbeat. Per-group DESCRIBE ACL is not applied by this
//! handler; topic-level and feature gates still run normally.

use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crabka_protocol::owned::common::streams_group_describe_response::assignment::Assignment;
use crabka_protocol::owned::common::streams_group_describe_response::key_value::KeyValue;
use crabka_protocol::owned::common::streams_group_describe_response::task_ids::TaskIds;
use crabka_protocol::owned::common::streams_group_describe_response::topic_info::TopicInfo;
use crabka_protocol::owned::streams_group_describe_request::StreamsGroupDescribeRequest;
use crabka_protocol::owned::streams_group_describe_response::{
    DescribedGroup, Member, StreamsGroupDescribeResponse, Subtopology, Topology,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::streams::actor::{
    StreamsDescribeMember, StreamsGroupActorMessage,
};
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let streams_enabled = broker.config.streams_group.enable;
    let image = broker.controller.current_image();
    let ng = broker.group_coordinator.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = StreamsGroupDescribeRequest::decode(&mut cur, version)?;

        // KIP-1071: same gate as the heartbeat — finalized streams.version >= 1
        // AND the config kill-switch. If disabled, each requested group gets a
        // GROUP_ID_NOT_FOUND error row (the protocol does not serve here).
        let enabled = crate::features::feature_enabled(&image, crate::features::STREAMS_VERSION, 1)
            && streams_enabled;

        let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
        for gid in &req.group_ids {
            if !enabled {
                groups.push(DescribedGroup {
                    group_id: gid.clone(),
                    error_code: codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                });
                continue;
            }
            // Per-group DESCRIBE ACL gate (Group resource) is not applied by
            // this plain 4-arg handler.
            let Some(handle) = ng.find_streams(gid) else {
                groups.push(DescribedGroup {
                    group_id: gid.clone(),
                    error_code: codes::GROUP_ID_NOT_FOUND,
                    ..Default::default()
                });
                continue;
            };
            let (tx, rx) = oneshot::channel();
            if handle
                .tx
                .send(StreamsGroupActorMessage::Describe { reply: tx })
                .await
                .is_err()
            {
                groups.push(DescribedGroup {
                    group_id: gid.clone(),
                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                    ..Default::default()
                });
                continue;
            }
            match rx.await {
                Ok(view) => groups.push(render_group(view)),
                Err(_) => groups.push(DescribedGroup {
                    group_id: gid.clone(),
                    error_code: codes::UNKNOWN_SERVER_ERROR,
                    ..Default::default()
                }),
            }
        }

        let resp = StreamsGroupDescribeResponse {
            groups,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

/// Map a [`StreamsDescribeView`] into a wire `DescribedGroup`.
///
/// [`StreamsDescribeView`]: crate::coordinator::unified::streams::actor::StreamsDescribeView
fn render_group(
    view: crate::coordinator::unified::streams::actor::StreamsDescribeView,
) -> DescribedGroup {
    DescribedGroup {
        error_code: codes::NONE,
        error_message: None,
        group_id: view.group_id,
        group_state: view.group_state,
        group_epoch: view.group_epoch,
        assignment_epoch: view.assignment_epoch,
        // The resolved topology (subtopologies + their topics). The real JVM
        // `DescribeStreamsGroupsHandler` errors on a response with no topology,
        // so render it whenever the group has one.
        topology: view.topology.map(render_topology),
        members: view.members.into_iter().map(render_member).collect(),
        // Per-group authorized-operations bitfield is not computed here, so
        // leave the wire default (INT32_MIN sentinel = "not set").
        ..Default::default()
    }
}

/// Map a describe-view member into a wire `Member`. The view carries current
/// (in-flight) active/standby/warmup task ownership; `target_assignment` is not
/// projected by the view so it renders empty.
fn render_member(m: StreamsDescribeMember) -> Member {
    Member {
        member_id: m.member_id,
        member_epoch: m.member_epoch,
        instance_id: m.instance_id,
        rack_id: m.rack_id,
        client_id: m.client_id,
        client_host: m.client_host,
        process_id: m.process_id,
        assignment: Assignment {
            active_tasks: task_map_to_ids(&m.active),
            standby_tasks: task_map_to_ids(&m.standby),
            warmup_tasks: task_map_to_ids(&m.warmup),
            ..Default::default()
        },
        // The view does not project the target (next) assignment, so render empty.
        ..Default::default()
    }
}

/// Map the stored `StreamsGroupTopologyValue` into the wire describe `Topology`.
/// The describe `Subtopology` omits the request-only `source_topic_regex` and
/// `copartition_groups`; everything else maps across field-for-field.
fn render_topology(
    t: crate::coordinator::unified::streams::persistence::StreamsGroupTopologyValue,
) -> Topology {
    use crate::coordinator::unified::streams::persistence::{StoredSubtopology, StoredTopicInfo};
    fn topic_info(ti: StoredTopicInfo) -> TopicInfo {
        TopicInfo {
            name: ti.name,
            partitions: ti.partitions,
            replication_factor: ti.replication_factor,
            topic_configs: ti
                .topic_configs
                .into_iter()
                .map(|(key, value)| KeyValue {
                    key,
                    value,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
    fn subtopology(s: StoredSubtopology) -> Subtopology {
        Subtopology {
            subtopology_id: s.subtopology_id,
            source_topics: s.source_topics,
            repartition_sink_topics: s.repartition_sink_topics,
            state_changelog_topics: s
                .state_changelog_topics
                .into_iter()
                .map(topic_info)
                .collect(),
            repartition_source_topics: s
                .repartition_source_topics
                .into_iter()
                .map(topic_info)
                .collect(),
            ..Default::default()
        }
    }
    Topology {
        epoch: t.epoch,
        subtopologies: Some(t.subtopologies.into_iter().map(subtopology).collect()),
        ..Default::default()
    }
}

/// Render a `subtopology -> partitions` task map as the response `Vec<TaskIds>`.
fn task_map_to_ids(map: &BTreeMap<String, Vec<i32>>) -> Vec<TaskIds> {
    map.iter()
        .map(|(sub, parts)| TaskIds {
            subtopology_id: sub.clone(),
            partitions: parts.clone(),
            ..Default::default()
        })
        .collect()
}
