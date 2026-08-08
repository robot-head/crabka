//! Resolve the response `TaskIds` into [`TaskAssignment`]s. Each assignment
//! carries the concrete source topic-partitions that its task reads, which come
//! from the built topology.

use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds;

use super::types::{TaskAssignment, TopicPartition};
use crate::topology::BuiltTopology;

/// Map one role's assigned tasks to [`TaskAssignment`]s. A `None`, which means
/// unchanged since the last heartbeat, gives an empty vec.
pub(crate) fn resolve(
    tasks: Option<&Vec<TaskIds>>,
    topology: &BuiltTopology,
) -> Vec<TaskAssignment> {
    let Some(tasks) = tasks else {
        return Vec::new();
    };
    tasks
        .iter()
        .map(|t| {
            let topics = topology.source_topics_for(&t.subtopology_id);
            let mut tps = Vec::new();
            for &p in &t.partitions {
                for topic in topics {
                    tps.push(TopicPartition {
                        topic: topic.clone(),
                        partition: p,
                    });
                }
            }
            TaskAssignment {
                subtopology_id: t.subtopology_id.clone(),
                partitions: t.partitions.clone(),
                source_topic_partitions: tps,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds;

    use super::*;
    use crate::topology::{NodeHandle, Topology};

    fn built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<bytes::Bytes, bytes::Bytes> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        t.build("app").unwrap()
    }

    #[test]
    fn resolves_task_to_source_topic_partitions() {
        let tasks = vec![TaskIds {
            subtopology_id: "0".into(),
            partitions: vec![0, 2],
            ..Default::default()
        }];
        let resolved = resolve(Some(&tasks), &built());
        check!(resolved.len() == 1);
        check!(resolved[0].subtopology_id == "0");
        check!(resolved[0].partitions == vec![0, 2]);
        let tps: Vec<(&str, i32)> = resolved[0]
            .source_topic_partitions
            .iter()
            .map(|tp| (tp.topic.as_str(), tp.partition))
            .collect();
        check!(tps == vec![("in", 0), ("in", 2)]);
    }

    #[test]
    fn none_resolves_to_empty() {
        check!(resolve(None, &built()).is_empty());
    }
}
