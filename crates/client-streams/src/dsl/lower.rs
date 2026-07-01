//! Lowering driver: walk the logical DSL graph and run each node's lowering
//! thunk to build the Processor-API [`Topology`].
//!
//! Nodes are visited in id order. Because [`LogicalGraph::add`] assigns ids
//! sequentially and a node's predecessors are passed in by id when it is added,
//! every predecessor has a strictly smaller id than its child — so id order is a
//! valid topological order (parents before children) for stateless chains,
//! merges, joins, repartitions, and table operations. A node whose thunk is
//! `None` is structural metadata and has no Processor-API node to lower.
use crate::dsl::graph::{GraphNodeKind, LogicalGraph, LowerState};

#[tracing::instrument(
    name = "streams.dsl.lower",
    level = "info",
    skip_all,
    fields(app_id = %app_id, nodes = graph.nodes.len()),
)]
pub(crate) fn lower(mut graph: LogicalGraph, app_id: &str) -> crate::topology::Topology {
    // REUSE_KTABLE_SOURCE_TOPICS: collect node id → source topic for every
    // TableSource the optimizer flagged, so its thunk can register its store with
    // the source topic as the changelog. Empty when the optimizer didn't run.
    let reuse_changelog = graph
        .nodes
        .iter()
        .filter_map(|n| match &n.kind {
            GraphNodeKind::TableSource {
                topic,
                reuse_source_for_changelog: true,
                ..
            } => Some((n.id, topic.clone())),
            _ => None,
        })
        .collect();

    let mut state = LowerState {
        topology: crate::topology::Topology::new(),
        app_id: app_id.to_string(),
        handle_name: std::collections::HashMap::new(),
        reuse_changelog,
    };
    let aliases = std::mem::take(&mut graph.aliases);
    for node in &mut graph.nodes {
        // An optimizer-aliased node (e.g. a redundant repartition merged into a
        // keeper) is *not* lowered: running its thunk would re-emit the merged
        // sink/topic/source. Instead inherit the keeper's lowered node name so
        // children that captured this node's id resolve to the shared node. The
        // keeper has a strictly lower id, so it was lowered earlier in this loop.
        if let Some(&target) = aliases.get(&node.id) {
            debug_assert!(
                target < node.id,
                "alias keeper id {target} must precede aliased id {} for id-order lowering",
                node.id
            );
            let name = state.handle_name[&target].clone();
            state.handle_name.insert(node.id, name);
            // Drop the redundant thunk without running it.
            node.lower.take();
            continue;
        }
        if let Some(thunk) = node.lower.take() {
            thunk(&mut state);
        }
    }
    state.topology
}
