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

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use crabka_protocol::owned::streams_group_describe_response as response_mod;
    use std::time::Duration;

    use crate::config::BrokerConfig;
    use crate::coordinator::unified::streams::actor::{
        StreamsDescribeMember, StreamsDescribeView, StreamsGroupActorMessage,
    };
    use crate::coordinator::unified::streams::persistence::{
        StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue,
    };

    fn request(group_ids: &[&str]) -> StreamsGroupDescribeRequest {
        StreamsGroupDescribeRequest {
            group_ids: group_ids.iter().map(|gid| (*gid).into()).collect(),
            ..Default::default()
        }
    }

    fn encode_request(req: &StreamsGroupDescribeRequest) -> Bytes {
        let version = response_mod::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> StreamsGroupDescribeResponse {
        let version = response_mod::MAX_VERSION;
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            StreamsGroupDescribeResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    async fn start_broker(
        streams_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.streams_group.enable = streams_enabled;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    async fn finalize_streams_version(broker: &Broker) {
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crate::features::STREAMS_VERSION.into(),
                level: 1,
            })])
            .await
            .expect("submit streams.version");

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if broker
                    .controller
                    .current_image()
                    .finalized_feature(crate::features::STREAMS_VERSION)
                    == Some(1)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("streams.version visible");
    }

    async fn describe(broker: &Broker, group_ids: &[&str]) -> StreamsGroupDescribeResponse {
        let version = response_mod::MAX_VERSION;
        let req_bytes = encode_request(&request(group_ids));
        let resp = handle(broker, version, 1, &req_bytes)
            .await
            .expect("handle describe");
        decode_response(&resp)
    }

    fn task_map(entries: &[(&str, Vec<i32>)]) -> BTreeMap<String, Vec<i32>> {
        entries
            .iter()
            .map(|(subtopology_id, partitions)| ((*subtopology_id).into(), partitions.clone()))
            .collect()
    }

    fn topology_value() -> StreamsGroupTopologyValue {
        StreamsGroupTopologyValue {
            epoch: 9,
            subtopologies: vec![StoredSubtopology {
                subtopology_id: "sub-a".into(),
                source_topics: vec!["input-a".into(), "input-b".into()],
                source_topic_regex: vec!["ignored-.*".into()],
                repartition_sink_topics: vec!["sink-a".into()],
                state_changelog_topics: vec![StoredTopicInfo {
                    name: "store-a-changelog".into(),
                    partitions: 3,
                    replication_factor: 2,
                    topic_configs: vec![("cleanup.policy".into(), "compact".into())],
                }],
                repartition_source_topics: vec![StoredTopicInfo {
                    name: "source-repartition".into(),
                    partitions: 4,
                    replication_factor: 1,
                    topic_configs: vec![("retention.ms".into(), "1000".into())],
                }],
                copartition_groups: Vec::new(),
            }],
        }
    }

    fn describe_member() -> StreamsDescribeMember {
        StreamsDescribeMember {
            member_id: "member-1".into(),
            member_epoch: 7,
            instance_id: Some("instance-a".into()),
            rack_id: Some("rack-a".into()),
            client_id: "client-a".into(),
            client_host: "/127.0.0.1".into(),
            process_id: "process-a".into(),
            active: task_map(&[("sub-a", vec![0, 2])]),
            standby: task_map(&[("sub-a", vec![1])]),
            warmup: task_map(&[("sub-b", vec![3, 4])]),
        }
    }

    #[tokio::test]
    async fn disabled_feature_returns_requested_group_error_rows() {
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();

        let resp = describe(&broker, &["g-disabled-a", "g-disabled-b"]).await;

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.groups.len() == 2, "{resp:?}");
        assert!(resp.groups[0].group_id == "g-disabled-a");
        assert!(resp.groups[0].error_code == codes::UNSUPPORTED_VERSION);
        assert!(resp.groups[1].group_id == "g-disabled-b");
        assert!(resp.groups[1].error_code == codes::UNSUPPORTED_VERSION);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn enabled_missing_group_returns_not_found_rows() {
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;

        let resp = describe(&broker, &["missing-a", "missing-b"]).await;

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.groups.len() == 2, "{resp:?}");
        assert!(resp.groups[0].group_id == "missing-a");
        assert!(resp.groups[0].error_code == codes::GROUP_ID_NOT_FOUND);
        assert!(resp.groups[1].group_id == "missing-b");
        assert!(resp.groups[1].error_code == codes::GROUP_ID_NOT_FOUND);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn closed_streams_actor_returns_load_in_progress_row() {
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;

        let actor = broker.group_coordinator.get_or_create_streams("stopped");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(StreamsGroupActorMessage::Shutdown(tx))
            .await
            .expect("send shutdown");
        rx.await.expect("actor shutdown");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !actor.tx.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actor sender closed");

        let resp = describe(&broker, &["stopped"]).await;

        assert!(resp.groups.len() == 1, "{resp:?}");
        assert!(resp.groups[0].group_id == "stopped");
        assert!(resp.groups[0].error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
        broker_handle.shutdown().await;
    }

    #[test]
    fn render_group_preserves_group_member_and_topology_fields() {
        let rendered = render_group(StreamsDescribeView {
            group_id: "streams-app".into(),
            group_epoch: 11,
            assignment_epoch: 10,
            topology_epoch: 9,
            group_state: "Stable".into(),
            topology: Some(topology_value()),
            members: vec![describe_member()],
        });

        assert!(rendered.group_id == "streams-app");
        assert!(rendered.error_code == codes::NONE);
        assert!(rendered.error_message.is_none());
        assert!(rendered.group_state == "Stable");
        assert!(rendered.group_epoch == 11);
        assert!(rendered.assignment_epoch == 10);
        assert!(rendered.topology.is_some());
        assert!(rendered.members.len() == 1, "{rendered:?}");

        let member = &rendered.members[0];
        assert!(member.member_id == "member-1");
        assert!(member.member_epoch == 7);
        assert!(member.instance_id.as_deref() == Some("instance-a"));
        assert!(member.rack_id.as_deref() == Some("rack-a"));
        assert!(member.client_id == "client-a");
        assert!(member.client_host == "/127.0.0.1");
        assert!(member.process_id == "process-a");
        assert!(member.assignment.active_tasks.len() == 1, "{member:?}");
        assert!(member.assignment.active_tasks[0].subtopology_id == "sub-a");
        assert!(member.assignment.active_tasks[0].partitions == vec![0, 2]);
        assert!(member.assignment.standby_tasks.len() == 1, "{member:?}");
        assert!(member.assignment.standby_tasks[0].subtopology_id == "sub-a");
        assert!(member.assignment.standby_tasks[0].partitions == vec![1]);
        assert!(member.assignment.warmup_tasks.len() == 1, "{member:?}");
        assert!(member.assignment.warmup_tasks[0].subtopology_id == "sub-b");
        assert!(member.assignment.warmup_tasks[0].partitions == vec![3, 4]);

        let topology = rendered.topology.as_ref().expect("topology");
        assert!(topology.epoch == 9);
        let subtopologies = topology.subtopologies.as_ref().expect("subtopologies");
        assert!(subtopologies.len() == 1, "{topology:?}");
        let sub = &subtopologies[0];
        assert!(sub.subtopology_id == "sub-a");
        assert!(sub.source_topics == vec!["input-a", "input-b"]);
        assert!(sub.repartition_sink_topics == vec!["sink-a"]);
        assert!(sub.state_changelog_topics.len() == 1, "{sub:?}");
        assert!(sub.state_changelog_topics[0].name == "store-a-changelog");
        assert!(sub.state_changelog_topics[0].partitions == 3);
        assert!(sub.state_changelog_topics[0].replication_factor == 2);
        assert!(sub.state_changelog_topics[0].topic_configs.len() == 1);
        assert!(sub.state_changelog_topics[0].topic_configs[0].key == "cleanup.policy");
        assert!(sub.state_changelog_topics[0].topic_configs[0].value == "compact");
        assert!(sub.repartition_source_topics.len() == 1, "{sub:?}");
        assert!(sub.repartition_source_topics[0].name == "source-repartition");
        assert!(sub.repartition_source_topics[0].partitions == 4);
        assert!(sub.repartition_source_topics[0].replication_factor == 1);
    }

    #[test]
    fn render_topology_preserves_subtopology_and_topic_info_fields() {
        let topology = render_topology(topology_value());

        assert!(topology.epoch == 9);
        let subtopologies = topology.subtopologies.as_ref().expect("subtopologies");
        assert!(subtopologies.len() == 1, "{topology:?}");
        let sub = &subtopologies[0];
        assert!(sub.subtopology_id == "sub-a");
        assert!(sub.source_topics == vec!["input-a", "input-b"]);
        assert!(sub.repartition_sink_topics == vec!["sink-a"]);
        assert!(sub.state_changelog_topics.len() == 1, "{sub:?}");
        assert!(sub.state_changelog_topics[0].name == "store-a-changelog");
        assert!(sub.state_changelog_topics[0].partitions == 3);
        assert!(sub.state_changelog_topics[0].replication_factor == 2);
        assert!(sub.state_changelog_topics[0].topic_configs.len() == 1);
        assert!(sub.state_changelog_topics[0].topic_configs[0].key == "cleanup.policy");
        assert!(sub.state_changelog_topics[0].topic_configs[0].value == "compact");
        assert!(sub.repartition_source_topics.len() == 1, "{sub:?}");
        assert!(sub.repartition_source_topics[0].name == "source-repartition");
        assert!(sub.repartition_source_topics[0].partitions == 4);
        assert!(sub.repartition_source_topics[0].replication_factor == 1);
        assert!(sub.repartition_source_topics[0].topic_configs.len() == 1);
        assert!(sub.repartition_source_topics[0].topic_configs[0].key == "retention.ms");
        assert!(sub.repartition_source_topics[0].topic_configs[0].value == "1000");
    }

    #[test]
    fn task_map_to_ids_preserves_sorted_task_maps() {
        let tasks = task_map_to_ids(&task_map(&[("z", vec![9]), ("a", vec![1, 2])]));

        assert!(tasks.len() == 2, "{tasks:?}");
        assert!(tasks[0].subtopology_id == "a");
        assert!(tasks[0].partitions == vec![1, 2]);
        assert!(tasks[1].subtopology_id == "z");
        assert!(tasks[1].partitions == vec![9]);
    }
}
