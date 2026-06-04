//! DSL optimizer passes (JVM `topology.optimization=all`).
//!
//! Each pass rewrites the [`LogicalGraph`] in place *before* lowering. The only
//! pass here is [`merge_repartition_topics`]; the `KTable` source-topic reuse
//! pass is added in a later task.

use std::collections::BTreeMap;

use crate::dsl::graph::{GraphNodeKind, LogicalGraph, NodeId};

/// `MERGE_REPARTITION_TOPICS`: when two aggregations hang off the *same*
/// key-changing operation, the JVM optimizer collapses their two repartition
/// topics into a single shared one (keeping the first aggregation's), so both
/// aggregations read one repartition and live in one subtopology.
///
/// We detect this by grouping the [`Repartition`] nodes by their predecessor
/// (the shared key-changing source). For each group of more than one, the
/// lowest-id node is the *keeper*; every other repartition node is recorded as
/// an alias of the keeper in [`LogicalGraph::aliases`]. The lowering driver then
/// skips the aliased nodes' sink/topic/source emission and points their handle
/// at the keeper's repartition source — so the redundant topic is never created
/// and the downstream aggregate reads the shared repartition. Each aggregate's
/// own state store (hence its changelog) is untouched, so both changelogs still
/// appear in the merged subtopology.
///
/// [`Repartition`]: GraphNodeKind::Repartition
pub(crate) fn merge_repartition_topics(graph: &mut LogicalGraph) {
    // predecessor id → repartition node ids sharing it (id-ascending via BTreeMap
    // keys + push order, since nodes are visited in id order below).
    let mut by_pred: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
    for node in &graph.nodes {
        if matches!(node.kind, GraphNodeKind::Repartition { .. })
            && let &[pred] = node.predecessors.as_slice()
        {
            by_pred.entry(pred).or_default().push(node.id);
        }
    }

    for (_pred, mut rp_ids) in by_pred {
        if rp_ids.len() < 2 {
            continue;
        }
        rp_ids.sort_unstable();
        let keeper = rp_ids[0];
        for &redundant in &rp_ids[1..] {
            graph.aliases.insert(redundant, keeper);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::graph::GraphNode;

    fn repartition_node(id: NodeId, pred: NodeId) -> GraphNode {
        GraphNode {
            id,
            name: format!("rp-{id}"),
            kind: GraphNodeKind::Repartition {
                topic: format!("topic-{id}"),
                partitions: None,
            },
            predecessors: vec![pred],
            children: Vec::new(),
            key_changing_operation: false,
            merge_node: false,
            lower: None,
            aux: None,
        }
    }

    #[test]
    fn two_repartitions_off_one_source_alias_to_the_lower_id() {
        let mut g = LogicalGraph::default();
        // node 0: the shared key-changing source (a placeholder processor).
        g.nodes.push(GraphNode {
            id: 0,
            name: "select-key".into(),
            kind: GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            predecessors: vec![],
            children: vec![],
            key_changing_operation: true,
            merge_node: false,
            lower: None,
            aux: None,
        });
        g.nodes.push(repartition_node(1, 0));
        g.nodes.push(repartition_node(2, 0));
        merge_repartition_topics(&mut g);
        assert_eq!(g.aliases.get(&2), Some(&1));
        assert_eq!(g.aliases.get(&1), None);
    }

    #[test]
    fn lone_repartition_is_not_aliased() {
        let mut g = LogicalGraph::default();
        g.nodes.push(GraphNode {
            id: 0,
            name: "select-key".into(),
            kind: GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            predecessors: vec![],
            children: vec![],
            key_changing_operation: true,
            merge_node: false,
            lower: None,
            aux: None,
        });
        g.nodes.push(repartition_node(1, 0));
        merge_repartition_topics(&mut g);
        assert!(g.aliases.is_empty());
    }

    #[test]
    fn repartitions_off_distinct_sources_do_not_merge() {
        let mut g = LogicalGraph::default();
        g.nodes.push(repartition_node(0, 100));
        g.nodes.push(repartition_node(1, 200));
        merge_repartition_topics(&mut g);
        assert!(g.aliases.is_empty());
    }
}
