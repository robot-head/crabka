//! Lowering driver: walk the logical DSL graph and run each node's lowering
//! thunk to build the Processor-API [`Topology`].
//!
//! Nodes are visited in id order. Because [`LogicalGraph::add`] assigns ids
//! sequentially and a node's predecessors are passed in by id when it is added,
//! every predecessor has a strictly smaller id than its child — so id order is a
//! valid topological order (parents before children) for the stateless chain and
//! merge. A node whose thunk is `None` (e.g. the repartition stub) is skipped;
//! later tasks attach those thunks and refine optimizer/branch ordering.
use crate::dsl::graph::{LogicalGraph, LowerState};

pub(crate) fn lower(mut graph: LogicalGraph, app_id: &str) -> crate::topology::Topology {
    let mut state = LowerState {
        topology: crate::topology::Topology::new(),
        app_id: app_id.to_string(),
        handle_name: std::collections::HashMap::new(),
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
