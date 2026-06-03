//! `GroupTopics` + `application_id` → the byte-exact `StreamsGroupHeartbeat`
//! wire `Topology`. Every ordering rule here matches the JVM 4.x client.

use crabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo;
use crabka_protocol::owned::streams_group_heartbeat_request::{
    CopartitionGroup, Subtopology, Topology,
};

use super::grouping::GroupTopics;

/// Build the wire `Topology` (epoch 0, sorted subtopologies + topic arrays).
#[allow(dead_code)]
pub(crate) fn to_wire(groups: &[GroupTopics], application_id: &str) -> Topology {
    let mut subtopologies: Vec<Subtopology> = groups
        .iter()
        .map(|g| subtopology(g, application_id))
        .collect();
    subtopologies.sort_by(|a, b| a.subtopology_id.cmp(&b.subtopology_id));
    Topology {
        epoch: 0,
        subtopologies,
        ..Default::default()
    }
}

fn subtopology(g: &GroupTopics, app: &str) -> Subtopology {
    let mut source_topics = g.source_topics.clone();
    source_topics.sort();
    let mut repartition_sink_topics = g.repartition_sink_topics.clone();
    repartition_sink_topics.sort();

    let mut repartition_source_topics: Vec<TopicInfo> = g
        .repartition_source_topics
        .iter()
        .map(|name| TopicInfo {
            name: name.clone(),
            partitions: 0,
            replication_factor: 0,
            ..Default::default()
        })
        .collect();
    repartition_source_topics.sort_by(|a, b| a.name.cmp(&b.name));

    let mut state_changelog_topics: Vec<TopicInfo> = g
        .changelog_stores
        .iter()
        .map(|store| TopicInfo {
            name: format!("{app}-{store}-changelog"),
            partitions: 0,
            replication_factor: 0,
            ..Default::default()
        })
        .collect();
    state_changelog_topics.sort_by(|a, b| a.name.cmp(&b.name));

    Subtopology {
        subtopology_id: g.id.clone(),
        source_topics,
        source_topic_regex: Vec::new(),
        state_changelog_topics,
        repartition_sink_topics,
        repartition_source_topics,
        copartition_groups: Vec::new(),
        ..Default::default()
    }
}

/// Encode a copartition group as `int16` indices into the sorted `sources` /
/// `repartition` arrays. Exposed (and unit-tested) so the byte-exact encoding is
/// covered even though the #1 builder emits no copartition groups.
#[allow(dead_code)]
pub(crate) fn copartition_group(
    sources: &[String],
    repartition: &[String],
    members: &[String],
) -> CopartitionGroup {
    let mut source_topics = Vec::new();
    let mut repartition_source_topics = Vec::new();
    for m in members {
        if let Some(i) = sources.iter().position(|s| s == m) {
            source_topics.push(i16::try_from(i).unwrap_or(i16::MAX));
        } else if let Some(i) = repartition.iter().position(|s| s == m) {
            repartition_source_topics.push(i16::try_from(i).unwrap_or(i16::MAX));
        }
    }
    source_topics.sort_unstable();
    repartition_source_topics.sort_unstable();
    CopartitionGroup {
        source_topics,
        source_topic_regex: Vec::new(),
        repartition_source_topics,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::grouping::GroupTopics;
    use assert2::check;

    #[test]
    fn epoch_is_zero_and_source_topics_sorted() {
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["b".into(), "a".into()],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "app");
        check!(topo.epoch == 0);
        check!(topo.subtopologies[0].source_topics == vec!["a".to_string(), "b".to_string()]);
        check!(topo.subtopologies[0].source_topic_regex.is_empty());
    }

    #[test]
    fn subtopologies_sort_by_id_as_string_not_numeric() {
        let groups = vec![
            GroupTopics {
                id: "2".into(),
                source_topics: vec!["x".into()],
                ..Default::default()
            },
            GroupTopics {
                id: "10".into(),
                source_topics: vec!["x".into()],
                ..Default::default()
            },
            GroupTopics {
                id: "1".into(),
                source_topics: vec!["x".into()],
                ..Default::default()
            },
        ];
        let topo = to_wire(&groups, "app");
        let ids: Vec<&str> = topo
            .subtopologies
            .iter()
            .map(|s| s.subtopology_id.as_str())
            .collect();
        check!(ids == vec!["1", "10", "2"]);
    }

    #[test]
    fn changelog_topics_named_and_zero_partitions() {
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec!["store".into()],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "my-app");
        let cl = &topo.subtopologies[0].state_changelog_topics;
        check!(cl.len() == 1);
        check!(cl[0].name == "my-app-store-changelog");
        check!(cl[0].partitions == 0);
    }

    #[test]
    fn copartition_indices_point_into_sorted_arrays() {
        let sources = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let repartition: Vec<String> = vec![];
        let cg = copartition_group(&sources, &repartition, &["c".into(), "a".into()]);
        check!(cg.source_topics == vec![0i16, 2i16]);
        check!(cg.repartition_source_topics.is_empty());
        check!(cg.source_topic_regex.is_empty());
    }
}
